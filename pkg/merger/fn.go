package main

func fn(inChans []chan byte, outChan chan byte) error {

	for {
		buf := make([]byte, 0, len(inChans))
		for _, inChan := range inChans {
			buf = append(buf, <-inChan)
		}

		//buf = sortByteSlice(buf)

		for _, b := range buf {
			outChan <- b
		}
	}
}

// sortByteSlice is a simple merge sort algorithm for runes
func sortByteSlice(buf []byte) []byte {
	if len(buf) <= 1 {
		return buf
	}

	mid := len(buf) / 2
	left := buf[:mid]
	right := buf[mid:]

	left = sortByteSlice(left)
	right = sortByteSlice(right)

	return merge(left, right)
}

func merge(left []byte, right []byte) []byte {
	res := make([]byte, len(left)+len(right))

	for len(left) > 0 && len(right) > 0 {
		if left[0] <= right[0] {
			res = append(res, left[0])
			left = left[1:]
		} else {
			res = append(res, right[0])
			right = right[1:]
		}
	}

	for len(left) > 0 {
		res = append(res, left[0])
		left = left[1:]
	}

	for len(right) > 0 {
		res = append(res, right[0])
		right = right[1:]
	}

	return res
}
