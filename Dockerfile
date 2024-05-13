# 使用 Go 1.22 版本的官方镜像
FROM golang:1.22-alpine as builder

# 设置工作目录，此处选择容器内的一个通用位置
WORKDIR /app

# 复制 go.mod 和 go.sum 文件到容器中
COPY ./go.mod .

# 下载所有依赖
RUN go mod download

# 复制项目的所有文件到容器中
COPY . .

# 编译项目，生成二进制文件
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o myapp main.go

# 使用基础的 alpine 镜像运行编译好的程序
FROM alpine:latest
WORKDIR /root/

# 从构建者镜像中复制二进制文件到当前目录
COPY --from=builder /app/myapp .

# 运行编译好的二进制文件
CMD ["./myapp"]
