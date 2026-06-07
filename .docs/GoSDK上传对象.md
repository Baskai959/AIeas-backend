普通上传是指通过 PutObjectV2 方法上传单个对象(Object)，支持上传字符串（字符流）、上传 Bytes（Bytes 流）、上传网络流和上传本地文件四种形式。
<span id="注意事项"></span>
## **注意事项**

* 上传对象前，您必须具有 `tos:PutObject` 权限，具体操作，请参见[权限配置指南](/docs/6349/102120)。
* 上传对象时，对象名必须满足一定规范，详细信息，请参见[对象命名规范](/docs/6349/74822)。
* TOS 是面向海量存储设计的分布式对象存储产品，内部分区存储了对象索引数据。为横向扩展您上传对象和下载对象时的最大吞吐量和减小热点分区的概率，请您避免使用字典序递增的对象命名方式，详细信息，请参见[性能优化](/docs/6349/155630)。
* 如果桶中已经存在同名对象，则新对象会覆盖已有的对象。如果您的桶开启了版本控制，则会保留原有对象，并生成一个新版本号用于标识新上传的对象。

<span id="示例代码"></span>
## **示例代码**
<span id="上传字符串"></span>
### **上传字符串**
您可以通过以下示例代码，使用 PutObjectV2 接口，上传字符串数据到 TOS 指定 `example_dir` 目录下的 `example.txt` 文件。
```go
package main

import (
   "context"
   "fmt"
   "strings"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 将字符串 “Hello TOS” 上传到指定 example_dir 目录下的 example.txt
   body := strings.NewReader("Hello TOS")
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
      },
      Content: body,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="上传网络流"></span>
### **上传网络流**
您可以通过以下示例代码，使用 PutObjectV2 接口上传网络流数据到 TOS 指定 `example_dir` 目录下的 `example.txt` 文件。
```go
package main

import (
   "context"
   "fmt"
   "net/http"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 从网络流中获取数据
   res, _ := http.Get("https://www.volcengine.com/")
   defer res.Body.Close()
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
      },
      Content: res.Body,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="上传本地文件流"></span>
### **上传本地文件流**
您可以通过以下示例代码，使用 PutObjectV2 接口，将指定路径上的文件上传到 TOS 指定 `example_dir` 目录下的 `example.txt` 文件。
```go
package main

import (
   "context"
   "fmt"
   "os"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      fileName = "/usr/local/test.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 读取本地文件数据
   f, err := os.Open("./example.txt")
   if err != nil {
      panic(err)
   }
   defer f.Close()
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
      },
      Content: f,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="从本地文件上传"></span>
### **从本地文件上传**
您可以通过以下示例代码，使用 PutObjectFromFile 接口，通过指定文件路径将文件上传到 TOS 指定 `example_dir` 目录下的 `example.txt` 文件。
```go
package main

import (
   "context"
   "fmt"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 直接使用文件路径上传文件
   output, err := client.PutObjectFromFile(ctx, &tos.PutObjectFromFileInput{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
      },
      FilePath: "./example.txt",
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="上传时设置对象元数据信息"></span>
### **上传时设置对象元数据信息**
您可以通过以下示例代码，使用 PutObjectV2 接口，上传字符串数据到指定 `example_dir` 目录下的 `example.txt` 文件，上传时指定对象存储类型为低频存储，权限为私有同时设置上传文件元数据信息。
```go
package main

import (
   "context"
   "fmt"
   "strings"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
   "github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 将字符串 “Hello TOS” 上传到指定 example_dir 目录下的 example.txt
   body := strings.NewReader("Hello TOS")
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
         // 指定存储类型为低频存储
         StorageClass: enum.StorageClassIa,
         // 指定对象权限为私有
         ACL: enum.ACLPrivate,
         // 用户自定义元数据信息
         Meta: map[string]string{"key": "value"},
      },
      Content: body,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="配置进度条"></span>
### **配置进度条**
上传时可通过实现 tos.DataTransferStatusChange 接口接收上传进度，代码示例如下。
```go
package main

import (
   "context"
   "fmt"
   "strings"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
   "github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

// 自定义进度回调，需要实现 tos.DataTransferStatusChange 接口
type listener struct {
}

func (l *listener) DataTransferStatusChange(event *tos.DataTransferStatus) {
   switch event.Type {
   case enum.DataTransferStarted:
      fmt.Println("Data transfer started")
   case enum.DataTransferRW:
      // Chunk 模式下 TotalBytes 值为 -1
      if event.TotalBytes != -1 {
         fmt.Printf("Once Read:%d,ConsumerBytes/TotalBytes: %d/%d,%d%%\n", event.RWOnceBytes, event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
      } else {
         fmt.Printf("Once Read:%d,ConsumerBytes:%d\n", event.RWOnceBytes, event.ConsumedBytes)
      }
   case enum.DataTransferSucceed:
      fmt.Printf("Data Transfer Succeed, ConsumerBytes/TotalBytes: %d/%d,%d%%\n", event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
   case enum.DataTransferFailed:
      fmt.Printf("Data Transfer Failed\n")
   }
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   // 将字符串 “Hello TOS” 上传到指定 example_dir 目录下的 example.txt
   body := strings.NewReader("Hello TOS")
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket: bucketName,
         Key:    objectKey,
         // 通过自定义方式设置回调函数查看上传进度
         DataTransferListener: &listener{},
      },
      Content: body,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="配置客户端限速"></span>
### **配置客户端限速**
上传对象时可以通过客户端使用 tos.RateLimiter 接口对上传数据所占用的带宽进行限制，代码如下所示。
```go
package main

import (
   "context"
   "fmt"
   "strings"
   "sync"
   "time"

   "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

func checkErr(err error) {
   if err != nil {
      if serverErr, ok := err.(*tos.TosServerError); ok {
         fmt.Println("Error:", serverErr.Error())
         fmt.Println("Request ID:", serverErr.RequestID)
         fmt.Println("Response Status Code:", serverErr.StatusCode)
         fmt.Println("Response Header:", serverErr.Header)
         fmt.Println("Response Err Code:", serverErr.Code)
         fmt.Println("Response Err Msg:", serverErr.Message)
      } else if clientErr, ok := err.(*tos.TosClientError); ok {
         fmt.Println("Error:", clientErr.Error())
         fmt.Println("Client Cause Err:", clientErr.Cause.Error())
      } else {
         fmt.Println("Error:", err)
      }
      panic(err)
   }
}

type rateLimit struct {
   rate     int64
   capacity int64

   currentAmount int64
   sync.Mutex
   lastConsumeTime time.Time
}

func NewDefaultRateLimit(rate int64, capacity int64) tos.RateLimiter {
   return &rateLimit{
      rate:            rate,
      capacity:        capacity,
      lastConsumeTime: time.Now(),
      currentAmount:   capacity,
      Mutex:           sync.Mutex{},
   }
}

func (d *rateLimit) Acquire(want int64) (ok bool, timeToWait time.Duration) {
   d.Lock()
   defer d.Unlock()
   if want > d.capacity {
      want = d.capacity
   }
   increment := int64(time.Now().Sub(d.lastConsumeTime).Seconds() * float64(d.rate))
   if increment+d.currentAmount > d.capacity {
      d.currentAmount = d.capacity
   } else {
      d.currentAmount += increment
   }
   if want > d.currentAmount {
      timeToWaitSec := float64(want-d.currentAmount) / float64(d.rate)
      return false, time.Duration(timeToWaitSec * float64(time.Second))
   }
   d.lastConsumeTime = time.Now()
   d.currentAmount -= want
   return true, 0
}

func main() {
   var (
      accessKey = os.Getenv("TOS_ACCESS_KEY")
      secretKey = os.Getenv("TOS_SECRET_KEY")
      // Bucket 对应的 Endpoint，以华北2（北京）为例：https://tos-cn-beijing.volces.com
      endpoint = "https://tos-cn-beijing.volces.com"
      region   = "cn-beijing"
      // 填写 BucketName
      bucketName = "*** Provide your bucket name ***"

      // 将文件上传到 example_dir 目录下的 example.txt 文件
      objectKey = "example_dir/example.txt"
      ctx       = context.Background()
   )
   // 初始化客户端
   client, err := tos.NewClientV2(endpoint, tos.WithRegion(region), tos.WithCredentials(tos.NewStaticCredentials(accessKey, secretKey)))
   checkErr(err)
   body := strings.NewReader("Hello TOS")
   rateLimit1M := 1024 * 1024
   // 上传对象并在客户端限制上传速度为 1M/s
   output, err := client.PutObjectV2(ctx, &tos.PutObjectV2Input{
      PutObjectBasicInput: tos.PutObjectBasicInput{
         Bucket:      bucketName,
         Key:         objectKey,
         RateLimiter: NewDefaultRateLimit(int64(rateLimit1M), int64(rateLimit1M)),
      },
      Content: body,
   })
   checkErr(err)
   fmt.Println("PutObjectV2 Request ID:", output.RequestID)
}
```

<span id="相关文档"></span>
## **相关文档**
关于上传对象的 API 文档，请参见 [PutObject](/docs/6349/74860)。

