package transformpipe

type TransformPipe interface {
	Pipe() (string, error)
}
