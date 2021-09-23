package main

/** build with 1.13 0.0.11 **/

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/stianeikeland/go-rpio"
	tb "gopkg.in/tucnak/telebot.v2"
)

var (
	// TeleTocken bot
	TeleToken = os.Getenv("TELE_TOKEN")
)

func main() {
	b, err := tb.NewBot(tb.Settings{
		// You can also set custom API URL.
		// If field is empty it equals to "https://api.telegram.org".
		URL: "",

		Token:  TeleToken,
		Poller: &tb.LongPoller{Timeout: 10 * time.Second},
	})

	if err != nil {
		log.Fatal(err)
		return
	}
		err = rpio.Open()
		if err != nil {
			panic(fmt.Sprint("unable to open gpio", err.Error()))
		}

		defer rpio.Close()

		b.Handle("/hello", func(m *tb.Message) {

			b.Send(m.Sender, "Hello I'm KBot! Call me simple Cabot")
		})

	b.Handle("/r", func(m *tb.Message) {
		pin := rpio.Pin(17)
		pin.Output()
		b.Send(m.Sender, "RED ON!")
		fmt.Println("RED ON")

	})
	b.Handle("/rr", func(m *tb.Message) {
		pin := rpio.Pin(17)
		pin.Input()
		b.Send(m.Sender, "RED OFF!")
		fmt.Println("RED OFF")

	})

	b.Handle("/a", func(m *tb.Message) {
		pin := rpio.Pin(27)
		pin.Output()
		b.Send(m.Sender, "AMBER ON!")
		fmt.Println("AMBER ON")

	})
	b.Handle("/aa", func(m *tb.Message) {
		pin := rpio.Pin(27)
		pin.Input()
		b.Send(m.Sender, "AMBER OFF!")
		fmt.Println("AMBER OFF")

	})

	b.Handle("/g", func(m *tb.Message) {
		pin := rpio.Pin(22)
		pin.Output()
		b.Send(m.Sender, "GREEN ON!")
		fmt.Println("GREEN ON")

	})

	b.Handle("/gg", func(m *tb.Message) {
		pin := rpio.Pin(22)
		pin.Input()
		b.Send(m.Sender, "GREEN OFF!")
		fmt.Println("GREEN OFF")

	})

	b.Start()
}
