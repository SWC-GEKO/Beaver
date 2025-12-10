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

func fn1(inputs map[string]*InputStream, output *OutputStream) error {
	windowSize := 32 * 1024

	buffers := make(map[string][]byte)

	log.Printf("started merge-function")

	for id := range inputs {
		buffers[id] = make([]byte, 0, windowSize)
	}

	readChunk := func(id string, in chan []byte) {
		log.Printf("trying to read chunks")
		select {
		case chunk := <-in:
			log.Printf("read chunk with length: %d from input-conn: %s", len(chunk), id)
			buffers[id] = append(buffers[id], chunk...)
			if len(buffers[id]) > windowSize {
				log.Printf("writing to buffer")
				buffers[id] = buffers[id][:windowSize]
			}
		default:
		}
	}

	for {
		for id, in := range inputs {
			log.Printf("reading from input: %s", id)
			readChunk(id, in.Ch)
		}

		total := 0
		for _, b := range buffers {
			total += len(b)
		}
		if total == 0 {
			break
		}

		merged := make([]byte, 0, total)

		for _, buf := range buffers {
			merged = append(buf)
		}

		log.Printf("merged buffers, total length: %d, cap of chan: %d", len(merged), cap(output.Ch))

		output.Ch <- merged

		for id := range buffers {
			buffers[id] = buffers[id][:0]
		}
		log.Printf("cleared all buffers")
	}

	return nil
}
