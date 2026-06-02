package tts

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDoubaoProtocolRoundTripFullClientEvent(t *testing.T) {
	frame, err := encodeDoubaoMessage(doubaoMessage{
		Type:      doubaoMsgFullClient,
		Flag:      doubaoFlagWithEvent,
		Event:     doubaoEventStartSession,
		SessionID: "session-1",
		Payload:   []byte(`{"ok":true}`),
	}, doubaoSerializationJSON)
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	msg, err := decodeDoubaoMessage(frame)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if msg.Type != doubaoMsgFullClient || msg.Event != doubaoEventStartSession || msg.SessionID != "session-1" || string(msg.Payload) != `{"ok":true}` {
		t.Fatalf("unexpected decoded message: %+v", msg)
	}
}

func TestDoubaoProtocolRoundTripAudioEvent(t *testing.T) {
	frame, err := encodeDoubaoMessage(doubaoMessage{
		Type:      doubaoMsgAudioOnlyServer,
		Flag:      doubaoFlagWithEvent,
		Event:     352,
		SessionID: "session-1",
		Payload:   []byte{0x01, 0x02, 0x03},
	}, doubaoSerializationRaw)
	if err != nil {
		t.Fatalf("encode audio message: %v", err)
	}
	msg, err := decodeDoubaoMessage(frame)
	if err != nil {
		t.Fatalf("decode audio message: %v", err)
	}
	if msg.Type != doubaoMsgAudioOnlyServer || msg.Event != 352 || msg.SessionID != "session-1" || len(msg.Payload) != 3 {
		t.Fatalf("unexpected decoded audio message: %+v", msg)
	}
}

func TestDoubaoProtocolRoundTripGzipJSONEvent(t *testing.T) {
	payload, err := gzipPayload([]byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("gzip payload: %v", err)
	}
	frame, err := encodeDoubaoMessage(doubaoMessage{
		Type:        doubaoMsgFullClient,
		Flag:        doubaoFlagWithEvent,
		Event:       doubaoEventSayHello,
		SessionID:   "session-1",
		Compression: doubaoCompressionGZIP,
		Payload:     payload,
	}, doubaoSerializationJSON)
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	msg, err := decodeDoubaoMessage(frame)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if msg.Type != doubaoMsgFullClient || msg.Event != doubaoEventSayHello || string(msg.Payload) != `{"ok":true}` {
		t.Fatalf("unexpected decoded message: %+v", msg)
	}
}

func TestDoubaoProtocolDecodeAudioSequenceFrame(t *testing.T) {
	var frame bytes.Buffer
	frame.Write([]byte{
		0x11,
		byte(doubaoMsgAudioOnlyServer<<4) | byte(0b001),
		byte(doubaoSerializationRaw << 4),
		0x00,
	})
	if err := binary.Write(&frame, binary.BigEndian, int32(1)); err != nil {
		t.Fatalf("write sequence: %v", err)
	}
	if err := binary.Write(&frame, binary.BigEndian, uint32(3)); err != nil {
		t.Fatalf("write payload size: %v", err)
	}
	frame.Write([]byte{0x01, 0x02, 0x03})

	msg, err := decodeDoubaoMessage(frame.Bytes())
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if msg.Type != doubaoMsgAudioOnlyServer || msg.Event != 0 || len(msg.Payload) != 3 {
		t.Fatalf("unexpected decoded message: %+v", msg)
	}
}
