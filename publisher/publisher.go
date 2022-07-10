package main

import (
	"fmt"
	"log"
    json "encoding/json"
    "math/rand"
    "time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)



var ModelStr = 
`{
  "order_uid": "b563feb7b2b84b6test",
  "track_number": "WBILMTESTTRACK",
  "entry": "WBIL",
  "delivery": {
    "name": "Test Testov",
    "phone": "+9720000000",
    "zip": "2639809",
    "city": "Kiryat Mozkin",
    "address": "Ploshad Mira 15",
    "region": "Kraiot",
    "email": "test@gmail.com"
  },
  "payment": {
    "transaction": "b563feb7b2b84b6test",
    "request_id": "",
    "currency": "USD",
    "provider": "wbpay",
    "amount": 1817,
    "payment_dt": 1637907727,
    "bank": "alpha",
    "delivery_cost": 1500,
    "goods_total": 317,
    "custom_fee": 0
  },
  "items": [
    {
      "chrt_id": 9934930,
      "track_number": "WBILMTESTTRACK",
      "price": 453,
      "rid": "ab4219087a764ae0btest",
      "name": "Mascaras",
      "sale": 30,
      "size": "0",
      "total_price": 317,
      "nm_id": 2389212,
      "brand": "Vivienne Sabo",
      "status": 202
    }
  ],
  "locale": "en",
  "internal_signature": "",
  "customer_id": "test",
  "delivery_service": "meest",
  "shardkey": "9",
  "sm_id": 99,
  "date_created": "2021-11-26T06:22:19Z",
  "oof_shard": "1"
}`

type Order struct {
    OrderUid          string  `json:"order_uid"`
    TrackNumber       string  `json:"track_number"`
    Entry             string  `json:"entry"`
    Delivery          Delivery_t `json:"delivery"`
    Payment           Payment_t  `json:"payment"`
    Items             []Items_t  `json:"items"`
    Locale            string    `json:"locale"`
    InternalSignature string    `json:"internal_signature"`
    CustomerId        string    `json:"customer_id"`
    DeliveryService   string    `json:"delivery_service"`
    Shardkey          string    `json:"shardkey"`
    SmId              int       `json:"sm_id"`
    DateCreated       string    `json:"date_created"`
    OofShard          string    `json:"oof_shard"`
}
type Delivery_t struct {
    Name              string `json:"name"`
    Phone             string `json:"phone"`
    Zip               string `json:"zip"`
    City              string `json:"city"`
    Address           string `json:"address"`
    Region            string `json:"region"`
    Email             string `json:"email"`
}

type Payment_t struct {
    Transaction       string `json:"transaction"`
    RequestId         string `json:"request_id"`
    Currency          string `json:"currency"`
    Provider          string `json:"provider"`
    Amount            int    `json:"amount"`
    PaymentDt         int    `json:"payment_dt"`
    Bank              string `json:"bank"`
    DeliveryCost      int    `json:"delivery_cost"`
    GoodsTotal        int    `json:"goods_total"`
    CustomFee         int    `json:"custom_fee"`
}

type Items_t struct {
    ChrtId            int64  `json:"chrt_id"`
    TrackNumberI      string `json:"track_number"`
    Price             int    `json:"price"`
    Rid               string `json:"rid"`
    NameI             string `json:"name"`
    Sale              int    `json:"sale"`
    Size              string `json:"size"`
    TotalPrice        int    `json:"total_price"`
    NmId              int    `json:"nm_id"`
    Brand             string `json:"brand"`
    Status            int    `json:"status"`
}

func main() {
    rand.Seed(time.Now().UnixNano())

    // Connect Options.
    opts := []nats.Option{nats.Name("NATS Streaming Example Publisher")}

	// Connect to NATS
	nc, err := nats.Connect(stan.DefaultNatsURL,opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	sc, err := stan.Connect("test-cluster","stan-pub",stan.NatsConn(nc))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err,stan.DefaultNatsURL)
	}
	defer sc.Close()

    var order Order
    err = json.Unmarshal([]byte(ModelStr),&order)
    if err != nil {
        fmt.Printf("%s\n",err)
        return
    }

    tmp := []byte(order.OrderUid)
    for i := 0; i < len(order.OrderUid)-4; i++ {
        if order.OrderUid[i] > 0x30 && order.OrderUid[i] < 0x39 {
            tmp[i] = byte(0x30 + rand.Intn(9))
        } else {
            tmp[i] = byte(97 + rand.Intn(6))
        }
    }
    order.OrderUid = string(tmp)
    fmt.Println("result orderUid:", order.OrderUid)

    tmp = []byte(order.Payment.Transaction)
    for i := 0; i < len(order.OrderUid)-4; i++ {
        if tmp[i] > 0x30 && tmp[i] < 0x39 {
            tmp[i] = byte(0x30 + rand.Intn(9))
        } else {
            tmp[i] = byte(0x61 + rand.Intn(6))
        }
    }
    order.Payment.Transaction = string(tmp)
    fmt.Println("result Transaction:", order.Payment.Transaction)

    tmp = []byte(order.Items[0].Rid)
    for i := 0; i < len(order.Items[0].Rid)-4; i++ {
        if tmp[i] > 0x30 && tmp[i] < 0x39 {
            tmp[i] = byte(0x30 + rand.Intn(9))
        } else {
            tmp[i] = byte(0x61 + rand.Intn(6))
        }
    }
    order.Payment.Transaction = string(tmp)
    fmt.Println("result Rid:", order.Items[0].Rid)

    msg,err := json.MarshalIndent(order,"","    ")
    if err != nil {
        fmt.Println(err)
        return
    } else {
        err = sc.Publish("ch",msg)
        if err != nil {
            log.Fatalf("Error during publish: %v\n", err)
        }
    }
}
