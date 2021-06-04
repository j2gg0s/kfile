package kfile

import (
	"fmt"
	"net/url"
	"strconv"
)

func parseURL(addr string) (*File, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("prase uri: %w", err)
	}
	f := File{
		Topic: u.Host,
	}

	q := u.Query()
	if v, ok := q["name"]; ok && len(v) > 0 {
		f.Name = []byte(v[0])
	} else {
		return nil, fmt.Errorf("name is reuqired: %s", addr)
	}

	if v, ok := q["partition"]; ok && len(v) > 0 {
		if i, err := strconv.ParseInt(v[0], 10, 32); err != nil {
			return nil, fmt.Errorf("invalid partition %s: %w", v[0], err)
		} else {
			f.partition = int32(i)
		}
	} else {
		return nil, fmt.Errorf("partition is required: %s", addr)
	}

	getInt64 := func(k string) (int64, error) {
		if v, ok := q[k]; ok && len(v) > 0 {
			if i, err := strconv.ParseInt(v[0], 10, 64); err != nil {
				return 0, fmt.Errorf("invalid %s %s: %w", k, v[0], err)
			} else {
				return i, nil
			}
		} else {
			return 0, fmt.Errorf("%s is required: %s", k, addr)
		}
	}
	if sOffset, err := getInt64("start"); err != nil {
		return nil, err
	} else {
		f.sOffset = sOffset
	}
	if eOffset, err := getInt64("end"); err != nil {
		return nil, err
	} else {
		f.eOffset = eOffset
	}

	return &f, nil
}

func formatURL(topic, name string) string {
	vals := url.Values{}
	vals.Set("name", name)
	vals.Set("partition", "0")
	vals.Set("start", "0")
	vals.Set("end", "0")
	return fmt.Sprintf("kfile://%s?%s", topic, vals.Encode())
}

func (f *File) URI() string {
	vals := url.Values{}
	vals.Set("name", string(f.Name))
	vals.Set("partition", strconv.Itoa(int(f.partition)))
	vals.Set("start", strconv.Itoa(int(f.sOffset)))
	vals.Set("end", strconv.Itoa(int(f.eOffset)))
	return fmt.Sprintf("kfile://%s?%s", f.Topic, vals.Encode())
}
