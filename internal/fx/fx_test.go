package fx

import "testing"

const (
	usd = 1_000_000
	eur = 920_000
	gbp = 790_000
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name            string
		cents, from, to int64
		want            int64
	}{
		{"usd to eur rounds up", 4990, usd, eur, 4591},
		{"usd to eur rounds down", 7, usd, eur, 6},
		{"half rounds up", 5, usd, 500_000, 3},
		{"below half rounds down", 3, usd, 400_000, 1},
		{"eur to gbp", 10_000, eur, gbp, 8587},
		{"usd to usd", 4990, usd, usd, 4990},
		{"zero stays zero", 0, usd, eur, 0},
		{"large amount", 1_000_000_000_000, usd, eur, 920_000_000_000},
		{"large amount between currencies", 1_000_000_000_000, eur, gbp, 858_695_652_174},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert(tt.cents, tt.from, tt.to)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if got != tt.want {
				t.Errorf("Convert(%d, %d, %d) = %d, want %d", tt.cents, tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestConvertDoesNotOverflow(t *testing.T) {
	// 4e18 cents × 500000 ppm is far above MaxInt64 before the division.
	got, err := Convert(4_000_000_000_000_000_000, usd, 500_000)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if want := int64(2_000_000_000_000_000_000); got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestConvertErrors(t *testing.T) {
	tests := []struct {
		name            string
		cents, from, to int64
	}{
		{"negative amount", -1, usd, eur},
		{"zero source rate", 100, 0, eur},
		{"result does not fit", 9_000_000_000_000_000_000, 1, 1_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Convert(tt.cents, tt.from, tt.to); err == nil {
				t.Error("Convert returned no error")
			}
		})
	}
}
