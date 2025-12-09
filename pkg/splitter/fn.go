package main

func fn(inputChan chan byte, outputChans []chan byte) error {
	count := 0
	for {
		select {
		case b := <-inputChan:
			length := len(outputChans)

			outIdx := count % length

			out := outputChans[outIdx]
			out <- b
			count++
		}
	}
}
