package mx

import "testing"

func Test_periodParse(t *testing.T) {
	type args struct {
		st string
		et string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"", args{"", ""}, "", "", true},
		{"", args{"你好吗", ""}, "", "", true},
		{"", args{"", "1997"}, "", "", true},
		{"", args{"1997-0", ""}, "", "", true},
		{"", args{"1997-1", ""}, "", "", true},
		{"", args{"1997-1-1", ""}, "", "", true},
		{"", args{"1997-1-11", ""}, "", "", true},
		{"1997@", args{"1997", ""}, "1997-01-01 00:00:00", "1998-01-01 00:00:00", false},
		{"1997@", args{"1997", "1998"}, "1997-01-01 00:00:00", "1999-01-01 00:00:00", false},
		{"1997@", args{"1997-01", ""}, "1997-01-01 00:00:00", "1997-02-01 00:00:00", false},
		{"1997@", args{"1997-01", "1997-02"}, "1997-01-01 00:00:00", "1997-03-01 00:00:00", false},
		{"1997@", args{"1997-01-01", ""}, "1997-01-01 00:00:00", "1997-01-02 00:00:00", false},
		{"1997@", args{"1997-01-01", "1997-01-02"}, "1997-01-01 00:00:00", "1997-01-03 00:00:00", false},
		{"1997@", args{"1997-01-01 01", ""}, "1997-01-01 01:00:00", "1997-01-01 02:00:00", false},
		{"1997@", args{"1997-01-01 01", "1997-01-01 02"}, "1997-01-01 01:00:00", "1997-01-01 03:00:00", false},
		{"1997@", args{"1997-01-01 01:01", ""}, "1997-01-01 01:01:00", "1997-01-01 01:02:00", false},
		{"1997@", args{"1997-01-01 01:01", "1997-01-01 01:02"}, "1997-01-01 01:01:00", "1997-01-01 01:03:00", false},
		{"1997@", args{"1997-01-01 01:01:01", ""}, "1997-01-01 01:01:01", "1997-01-01 01:01:02", false},
		{"1997@", args{"1997-01-01 01:01:01", "1997-01-01 01:01:02"}, "1997-01-01 01:01:01", "1997-01-01 01:01:03", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := periodParse(tt.args.st, tt.args.et)
			if (err != nil) != tt.wantErr {
				t.Errorf("periodParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("periodParse() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("periodParse() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
