package main

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
	TeleTocken = os.Getenv("TELE_TOKEN")
)

func main() {
	b, err := tb.NewBot(tb.Settings{
		// You can also set custom API URL.
		// If field is empty it equals to "https://api.telegram.org".
		URL: "",

		Token:  TeleTocken,
		Poller: &tb.LongPoller{Timeout: 10 * time.Second},
	})

	if err != nil {
		log.Fatal(err)
		return
	}

	b.Handle("/hello", func(m *tb.Message) {

		b.Send(m.Sender, "Hello I'm KBot! Call me simple Cabot")
	})

	b.Handle("/red", func(m *tb.Message) {
		fmt.Println("opening gpio")
		err := rpio.Open()
		if err != nil {
			panic(fmt.Sprint("unable to open gpio", err.Error()))
		}

		defer rpio.Close()
		pin := rpio.Pin(18)
		pin.Output()

		pin.Toggle()

		b.Send(m.Sender, "Hello World!")
	})

	b.Start()
}
