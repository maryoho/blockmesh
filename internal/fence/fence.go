package fence

import "context"

// Fencer must guarantee that the target node can no longer issue reads or
// writes to the protected SAN LUN through ANY path before returning success.
type Fencer interface {
	FenceNode(context.Context, string, string) error
}

// NoopFencer is for unit tests only. Never wire it into a production daemon.
type NoopFencer struct{}

func (NoopFencer) FenceNode(context.Context, string, string) error { return nil }
