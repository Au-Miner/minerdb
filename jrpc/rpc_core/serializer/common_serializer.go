package serializer

const (
	JSONSerializer     = iota // 0
	KRYOSerializer            // 1
	HESSIANSerializer         // 2
	PROTOBUFSerializer        // 3
)

type CommonSerializer interface {
	Serialize(obj interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
	GetCode() int
}

func GetByCode(code int) CommonSerializer {
	switch code {
	case JSONSerializer:
		return &JsonSerializer{}
	default:
		return nil
	}
}
