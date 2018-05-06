package dumper

import (
	"testing"
)

func TestGetDumper(t *testing.T) {
	GetDumper("fs", map[interface{}]interface{}{})
}
