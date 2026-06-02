package tts

import (
	"bytes"
	"compress/gzip"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	appconfig "aieas_backend/internal/config"
	"aieas_backend/internal/domain"
	"aieas_backend/internal/service"

	"github.com/gorilla/websocket"
)

const (
	doubaoRealtimeURL   = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"
	doubaoResourceID    = "volc.speech.dialog"
	doubaoAppKey        = "PlgvMymc7f3tQnJ6"
	doubaoProvider      = "doubao"
	doubaoDefaultModel  = "1.2.1.1"
	doubaoAudioFormat   = "pcm_s16le"
	doubaoAudioEncoding = "pcm_s16le"
	doubaoSampleRate    = 24000
	doubaoChannels      = 1
	doubaoRecvTimeout   = 120

	defaultSynthesisTimeout = 120 * time.Second
	defaultWriteTimeout     = 5 * time.Second
)

// DoubaoClient 通过豆包 RealtimeAPI 把文本合成为直播播报音频。
type DoubaoClient struct {
	cfg     appconfig.DoubaoTTSConfig
	dialer  *websocket.Dialer
	timeout time.Duration
}

func NewDoubaoClient(cfg appconfig.DoubaoTTSConfig) *DoubaoClient {
	return &DoubaoClient{
		cfg:     cfg,
		dialer:  websocket.DefaultDialer,
		timeout: defaultSynthesisTimeout,
	}
}

func (c *DoubaoClient) SynthesizeLiveVoice(ctx context.Context, in service.LiveVoiceSynthesisInput) (service.LiveVoiceSynthesisResult, error) {
	if c == nil {
		return service.LiveVoiceSynthesisResult{}, domain.ErrInvalidState
	}
	text := strings.TrimSpace(in.Text)
	if text == "" {
		return service.LiveVoiceSynthesisResult{}, domain.ErrInvalidArgument
	}
	appID := strings.TrimSpace(c.cfg.AppID)
	ackToken := strings.TrimSpace(c.cfg.AckToken)
	voice := strings.TrimSpace(c.cfg.Voice)
	if appID == "" || ackToken == "" || voice == "" {
		return service.LiveVoiceSynthesisResult{}, fmt.Errorf("doubao tts is not configured: %w", domain.ErrInvalidState)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	conn, err := c.dial(ctx, appID, ackToken)
	if err != nil {
		return service.LiveVoiceSynthesisResult{}, err
	}
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	sessionID := randomHexID(16)
	if err := c.startConnection(ctx, conn); err != nil {
		return service.LiveVoiceSynthesisResult{}, err
	}
	if err := c.startSession(ctx, conn, sessionID, voice); err != nil {
		return service.LiveVoiceSynthesisResult{}, err
	}
	defer func() {
		_ = c.sendEvent(context.Background(), conn, doubaoEventFinishSession, sessionID, []byte("{}"), doubaoSerializationJSON)
		_ = c.sendEvent(context.Background(), conn, doubaoEventFinishConnection, "", []byte("{}"), doubaoSerializationJSON)
	}()

	if err := c.sendSayHello(ctx, conn, sessionID, text); err != nil {
		return service.LiveVoiceSynthesisResult{}, err
	}
	audio, err := c.readAudio(ctx, conn)
	if err != nil {
		return service.LiveVoiceSynthesisResult{}, err
	}
	return service.LiveVoiceSynthesisResult{
		Audio:       audio,
		AudioFormat: doubaoAudioFormat,
		Encoding:    doubaoAudioEncoding,
		SampleRate:  doubaoSampleRate,
		Channels:    doubaoChannels,
		Voice:       voice,
		Provider:    doubaoProvider,
	}, nil
}

func (c *DoubaoClient) dial(ctx context.Context, appID, ackToken string) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("X-Api-Resource-Id", doubaoResourceID)
	header.Set("X-Api-Access-Key", ackToken)
	header.Set("X-Api-App-Key", doubaoAppKey)
	header.Set("X-Api-App-ID", appID)
	header.Set("X-Api-Connect-Id", randomHexID(16))
	conn, _, err := c.dialer.DialContext(ctx, doubaoRealtimeURL, header)
	if err != nil {
		return nil, fmt.Errorf("dial doubao realtime api: %w", err)
	}
	return conn, nil
}

func (c *DoubaoClient) startConnection(ctx context.Context, conn *websocket.Conn) error {
	if err := c.sendEvent(ctx, conn, doubaoEventStartConnection, "", []byte("{}"), doubaoSerializationJSON); err != nil {
		return err
	}
	msg, err := c.readMessage(ctx, conn)
	if err != nil {
		return fmt.Errorf("read doubao connection started: %w", err)
	}
	if msg.Type != doubaoMsgFullServer || msg.Event != doubaoEventConnectionStarted {
		return fmt.Errorf("unexpected doubao connection response event=%d type=%d: %w", msg.Event, msg.Type, domain.ErrInvalidState)
	}
	return nil
}

func (c *DoubaoClient) startSession(ctx context.Context, conn *websocket.Conn, sessionID, voice string) error {
	payload, err := json.Marshal(startSessionPayload{
		ASR: asrPayload{
			Extra: map[string]interface{}{
				"end_smooth_window_ms": 1500,
			},
		},
		TTS: ttsPayload{
			Speaker: voice,
			AudioConfig: audioConfig{
				Channel:    doubaoChannels,
				Format:     doubaoAudioFormat,
				SampleRate: doubaoSampleRate,
			},
		},
		Dialog: dialogPayload{
			BotName:       "豆包",
			SystemRole:    "你使用自然清晰的中文声音。",
			SpeakingStyle: "你的说话风格简洁明了，语速适中，语调自然。",
			Location:      map[string]string{"city": "北京"},
			Extra: map[string]interface{}{
				"strict_audit": false,
				"recv_timeout": doubaoRecvTimeout,
				"input_mod":    "text",
				"model":        doubaoDefaultModel,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal doubao start session payload: %w", err)
	}
	if err := c.sendEvent(ctx, conn, doubaoEventStartSession, sessionID, payload, doubaoSerializationJSON); err != nil {
		return err
	}
	msg, err := c.readMessage(ctx, conn)
	if err != nil {
		return fmt.Errorf("read doubao session started: %w", err)
	}
	if msg.Type != doubaoMsgFullServer || msg.Event != doubaoEventSessionStarted {
		return fmt.Errorf("unexpected doubao session response event=%d type=%d: %w", msg.Event, msg.Type, domain.ErrInvalidState)
	}
	return nil
}

func (c *DoubaoClient) sendSayHello(ctx context.Context, conn *websocket.Conn, sessionID, text string) error {
	payload, err := json.Marshal(sayHelloPayload{Content: text})
	if err != nil {
		return fmt.Errorf("marshal doubao say hello payload: %w", err)
	}
	return c.sendEvent(ctx, conn, doubaoEventSayHello, sessionID, payload, doubaoSerializationJSON)
}

func (c *DoubaoClient) readAudio(ctx context.Context, conn *websocket.Conn) ([]byte, error) {
	var audio []byte
	for {
		msg, err := c.readMessage(ctx, conn)
		if err != nil {
			return nil, err
		}
		switch msg.Type {
		case doubaoMsgAudioOnlyServer:
			if len(msg.Payload) > 0 {
				audio = append(audio, msg.Payload...)
			}
		case doubaoMsgFullServer:
			switch msg.Event {
			case doubaoEventTTSEnded:
				if len(audio) == 0 {
					return nil, fmt.Errorf("doubao returned empty tts audio: %w", domain.ErrInvalidState)
				}
				return audio, nil
			case doubaoEventConnectionFailed, doubaoEventSessionFailed, doubaoEventSessionFinished:
				return nil, fmt.Errorf("doubao session ended event=%d payload=%s: %w", msg.Event, string(msg.Payload), domain.ErrInvalidState)
			}
		case doubaoMsgError:
			return nil, fmt.Errorf("doubao error code=%d payload=%s: %w", msg.ErrorCode, string(msg.Payload), domain.ErrInvalidState)
		}
	}
}

func (c *DoubaoClient) sendEvent(ctx context.Context, conn *websocket.Conn, event int32, sessionID string, payload []byte, serialization doubaoSerialization) error {
	compression := doubaoCompressionNone
	if serialization == doubaoSerializationJSON {
		var err error
		payload, err = gzipPayload(payload)
		if err != nil {
			return fmt.Errorf("gzip doubao event %d payload: %w", event, err)
		}
		compression = doubaoCompressionGZIP
	}
	frame, err := encodeDoubaoMessage(doubaoMessage{
		Type:        doubaoMsgFullClient,
		Flag:        doubaoFlagWithEvent,
		Event:       event,
		SessionID:   sessionID,
		Compression: compression,
		Payload:     payload,
	}, serialization)
	if err != nil {
		return fmt.Errorf("encode doubao event %d: %w", event, err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	} else {
		_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return fmt.Errorf("send doubao event %d: %w", event, err)
	}
	return nil
}

func (c *DoubaoClient) readMessage(ctx context.Context, conn *websocket.Conn) (doubaoMessage, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	mt, frame, err := conn.ReadMessage()
	if err != nil {
		if ctx.Err() != nil {
			return doubaoMessage{}, ctx.Err()
		}
		return doubaoMessage{}, fmt.Errorf("read doubao message: %w", err)
	}
	if mt != websocket.BinaryMessage && mt != websocket.TextMessage {
		return doubaoMessage{}, fmt.Errorf("unexpected doubao websocket message type %d: %w", mt, domain.ErrInvalidState)
	}
	msg, err := decodeDoubaoMessage(frame)
	if err != nil {
		return doubaoMessage{}, err
	}
	if msg.Type == doubaoMsgError {
		return msg, nil
	}
	return msg, nil
}

type startSessionPayload struct {
	ASR    asrPayload    `json:"asr"`
	TTS    ttsPayload    `json:"tts"`
	Dialog dialogPayload `json:"dialog"`
}

type asrPayload struct {
	Extra map[string]interface{} `json:"extra"`
}

type ttsPayload struct {
	Speaker     string      `json:"speaker"`
	AudioConfig audioConfig `json:"audio_config"`
}

type audioConfig struct {
	Channel    int    `json:"channel"`
	Format     string `json:"format"`
	SampleRate int    `json:"sample_rate"`
}

type dialogPayload struct {
	BotName       string                 `json:"bot_name"`
	SystemRole    string                 `json:"system_role"`
	SpeakingStyle string                 `json:"speaking_style"`
	Location      map[string]string      `json:"location,omitempty"`
	Extra         map[string]interface{} `json:"extra"`
}

type sayHelloPayload struct {
	Content string `json:"content"`
}

type doubaoMsgType uint8
type doubaoMsgFlag uint8
type doubaoSerialization uint8
type doubaoCompression uint8

const (
	doubaoMsgFullClient      doubaoMsgType = 0x1
	doubaoMsgAudioOnlyServer doubaoMsgType = 0xb
	doubaoMsgFullServer      doubaoMsgType = 0x9
	doubaoMsgError           doubaoMsgType = 0xf

	doubaoFlagSequenceMask doubaoMsgFlag = 0b011
	doubaoFlagWithEvent    doubaoMsgFlag = 0b100

	doubaoSerializationRaw  doubaoSerialization = 0
	doubaoSerializationJSON doubaoSerialization = 1

	doubaoCompressionNone doubaoCompression = 0
	doubaoCompressionGZIP doubaoCompression = 1
)

const (
	doubaoEventStartConnection   int32 = 1
	doubaoEventFinishConnection  int32 = 2
	doubaoEventConnectionStarted int32 = 50
	doubaoEventConnectionFailed  int32 = 51
	doubaoEventStartSession      int32 = 100
	doubaoEventFinishSession     int32 = 102
	doubaoEventSessionStarted    int32 = 150
	doubaoEventSessionFailed     int32 = 153
	doubaoEventSessionFinished   int32 = 152
	doubaoEventSayHello          int32 = 300
	doubaoEventTTSEnded          int32 = 359
)

type doubaoMessage struct {
	Type          doubaoMsgType
	Flag          doubaoMsgFlag
	Serialization doubaoSerialization
	Compression   doubaoCompression
	Event         int32
	SessionID     string
	ConnectID     string
	ErrorCode     uint32
	Payload       []byte
}

func encodeDoubaoMessage(msg doubaoMessage, serialization doubaoSerialization) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{
		0x11,
		byte(msg.Type<<4) | byte(msg.Flag),
		byte(serialization<<4) | byte(msg.Compression),
		0x00,
	})
	if msg.Flag&doubaoFlagWithEvent == doubaoFlagWithEvent {
		if err := binary.Write(buf, binary.BigEndian, msg.Event); err != nil {
			return nil, err
		}
		if !doubaoEventSkipsSessionID(msg.Event) {
			if err := writeSizePrefixedString(buf, msg.SessionID); err != nil {
				return nil, err
			}
		}
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(msg.Payload))); err != nil {
		return nil, err
	}
	_, _ = buf.Write(msg.Payload)
	return buf.Bytes(), nil
}

func decodeDoubaoMessage(frame []byte) (doubaoMessage, error) {
	if len(frame) < 4 {
		return doubaoMessage{}, fmt.Errorf("doubao frame header too short: %w", domain.ErrInvalidState)
	}
	headerSize := int(frame[0]&0x0f) * 4
	if headerSize < 4 || len(frame) < headerSize {
		return doubaoMessage{}, fmt.Errorf("invalid doubao header size %d: %w", headerSize, domain.ErrInvalidState)
	}
	msg := doubaoMessage{
		Type:          doubaoMsgType(frame[1] >> 4),
		Flag:          doubaoMsgFlag(frame[1] & 0x0f),
		Serialization: doubaoSerialization(frame[2] >> 4),
		Compression:   doubaoCompression(frame[2] & 0x0f),
	}
	buf := bytes.NewReader(frame[headerSize:])
	if msg.Type == doubaoMsgError {
		if err := binary.Read(buf, binary.BigEndian, &msg.ErrorCode); err != nil {
			return doubaoMessage{}, fmt.Errorf("read doubao error code: %w", err)
		}
		if err := readDoubaoPayload(buf, &msg); err != nil {
			return doubaoMessage{}, err
		}
		return msg, nil
	}
	if msg.Flag&doubaoFlagSequenceMask != 0 {
		var sequence int32
		if err := binary.Read(buf, binary.BigEndian, &sequence); err != nil {
			return doubaoMessage{}, fmt.Errorf("read doubao sequence: %w", err)
		}
	}
	if msg.Flag&doubaoFlagWithEvent == doubaoFlagWithEvent {
		if err := binary.Read(buf, binary.BigEndian, &msg.Event); err != nil {
			return doubaoMessage{}, fmt.Errorf("read doubao event: %w", err)
		}
		if !doubaoEventSkipsSessionID(msg.Event) {
			sessionID, err := readSizePrefixedString(buf)
			if err != nil {
				return doubaoMessage{}, fmt.Errorf("read doubao session id: %w", err)
			}
			msg.SessionID = sessionID
		}
		if doubaoEventHasConnectID(msg.Event) {
			connectID, err := readSizePrefixedString(buf)
			if err != nil {
				return doubaoMessage{}, fmt.Errorf("read doubao connect id: %w", err)
			}
			msg.ConnectID = connectID
		}
	}
	if err := readDoubaoPayload(buf, &msg); err != nil {
		return doubaoMessage{}, err
	}
	return msg, nil
}

func readDoubaoPayload(buf *bytes.Reader, msg *doubaoMessage) error {
	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return fmt.Errorf("read doubao payload size: %w", err)
	}
	if uint64(size) > uint64(buf.Len()) {
		return fmt.Errorf("doubao payload size %d exceeds remaining %d: %w", size, buf.Len(), domain.ErrInvalidState)
	}
	if size > 0 {
		payload := make([]byte, int(size))
		if _, err := buf.Read(payload); err != nil {
			return fmt.Errorf("read doubao payload: %w", err)
		}
		decoded, err := decodeDoubaoPayload(payload, msg.Compression)
		if err != nil {
			return err
		}
		msg.Payload = decoded
	}
	return nil
}

func gzipPayload(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(payload); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeDoubaoPayload(payload []byte, compression doubaoCompression) ([]byte, error) {
	switch compression {
	case doubaoCompressionNone:
		return payload, nil
	case doubaoCompressionGZIP:
		zr, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("create doubao gzip reader: %w", err)
		}
		defer zr.Close()
		decoded, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("read doubao gzip payload: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported doubao compression %d: %w", compression, domain.ErrInvalidState)
	}
}

func writeSizePrefixedString(buf *bytes.Buffer, value string) error {
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, _ = buf.WriteString(value)
	return nil
}

func readSizePrefixedString(buf *bytes.Reader) (string, error) {
	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return "", err
	}
	if uint64(size) > uint64(buf.Len()) {
		return "", fmt.Errorf("string size %d exceeds remaining %d", size, buf.Len())
	}
	if size == 0 {
		return "", nil
	}
	b := make([]byte, int(size))
	if _, err := buf.Read(b); err != nil {
		return "", err
	}
	return string(b), nil
}

func doubaoEventSkipsSessionID(event int32) bool {
	switch event {
	case doubaoEventStartConnection, doubaoEventFinishConnection, doubaoEventConnectionStarted, doubaoEventConnectionFailed, 52:
		return true
	default:
		return false
	}
}

func doubaoEventHasConnectID(event int32) bool {
	switch event {
	case doubaoEventConnectionStarted, doubaoEventConnectionFailed, 52:
		return true
	default:
		return false
	}
}

func randomHexID(n int) string {
	if n <= 0 {
		n = 16
	}
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
