package storage

import (
	"testing"

	"github.com/catlev/pkg/domain"
)

func TestPartsToString(t *testing.T) {
	repeat := func(b domain.Word) domain.Word {
		var r domain.Word
		for i := 0; i < 8; i++ {
			r |= b << (i * 8)
		}
		return r
	}

	for _, test := range []struct {
		name  string
		parts []domain.Word
		str   string
	}{
		{
			name:  "Empty",
			parts: nil,
			str:   "",
		},
		{
			name:  "AlsoEmpty",
			parts: []domain.Word{0},
			str:   "",
		},
		{
			name:  "Short",
			parts: []domain.Word{'i'<<8 | 'h'},
			str:   "hi",
		},
		{
			name:  "Longer",
			parts: []domain.Word{repeat('h'), repeat('o') - 1},
			str:   "hhhhhhhhnooooooo",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			str := partsToString(test.parts)
			if str != test.str {
				t.Errorf("got %s, expected %s", str, test.str)
			}
		})
	}
}
