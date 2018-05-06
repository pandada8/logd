package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStringBy(t *testing.T) {
	source := map[interface{}]interface{}{
		"name": "????",
		"pattern": map[interface{}]interface{}{
			"xxxx": "yy",
			"yy":   "xxxx",
		},
	}
	assert.Equal(t, "????", GetStringBy(source, "name"))
	assert.Equal(t, "", GetStringBy(source, "pattern.xxx"))
	assert.Equal(t, "xxxx", GetStringBy(source, "pattern.yy"))
}

func TestGetBy(t *testing.T) {
	sub := map[interface{}]interface{}{
		"xxxx": "yy",
		"yy":   "xxxx",
	}

	source := map[interface{}]interface{}{
		"name":    "????",
		"pattern": sub,
	}
	assert.Equal(t, "????", GetBy(source, "name"))
	assert.Equal(t, sub, GetBy(source, "pattern"))
	assert.Equal(t, nil, GetBy(source, "????"))
}
