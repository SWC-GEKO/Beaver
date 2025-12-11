package main

import (
	"context"
	"log"
	"time"
)

func fn(ctx context.Context, inputs []chan []byte, output chan []byte) error {
	const windowSize = 32 * 1024
	buffers := make([][]byte, len(inputs))

	for i := range buffers {
		buffers[i] = make([]byte, 0, windowSize)
	}

	log.Printf("started merge-function")

	for {
		select {
		case <-ctx.Done():
			log.Printf("canceled merge function, draining")
			for {
				drained := false
				for i, ch := range inputs {
					select {
					case chunk, ok := <-ch:
						if ok {
							buffers[i] = append(buffers[i], chunk...)
							drained = true
						}
					default:
					}
				}

				if !drained {
					break
				}
			}

			total := 0
			for _, buf := range buffers {
				total += len(buf)
			}

			if total > 0 {
				merged := make([]byte, 0, total)
				for _, buf := range buffers {
					merged = append(merged, buf...)
				}

				output <- merged
				log.Printf("flushed merged bytes, len: %d", len(merged))
			}

			return nil

		default:
			total := 0

			for i, ch := range inputs {
				select {
				case chunk, ok := <-ch:
					if !ok {
						continue
					}

					buffers[i] = append(buffers[i], chunk...)
					if len(buffers[i]) > windowSize {
						buffers[i] = buffers[i][:windowSize]
					}
				default:
				}

				total += len(buffers[i])
			}

			if total == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			merged := make([]byte, 0, total)
			for _, buf := range buffers {
				merged = append(merged, buf...)
			}

			output <- merged

			for i := range buffers {
				buffers[i] = buffers[i][:0]
			}
		}
	}
}
