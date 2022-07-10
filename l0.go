package main

import (
	"context"
	json "encoding/json"
	"fmt"
	"os"
    http "net/http"

	"os/signal"

	"github.com/jackc/pgx/v4"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
)

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

var cache map[string][]byte

func main() {

    cache = make(map[string][]byte)

    //database
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
    defer conn.Close(context.Background())

    var orderUids []string
    var uid string

    uids,err := conn.Query(context.Background(),`select order_uid from json_order;`)
    if err != nil {
        fmt.Println(err)
    }

    for uids.Next() {
        err = uids.Scan(&uid)
        if err == nil {
            orderUids = append(orderUids,uid)
        } else {
            fmt.Println(err)
        }
    }

    uids.Close()

    for i := range orderUids {
        var tmpOrder Order
        var orderItems []Items_t

        items,err := conn.Query(context.Background(), 
       `select chrt_id, track_number_i, price, rid, name_i, 
        sale, size, total_price, nm_id, brand, status from items

        where order_uid_i = $1`, orderUids[i])
        defer items.Close()

        if err != nil {
            fmt.Println(err)
        }

        for items.Next() {
            var tmpItem Items_t
            err = items.Scan(&tmpItem.ChrtId,
                             &tmpItem.TrackNumberI,
                             &tmpItem.Price,
                             &tmpItem.Rid,
                             &tmpItem.NameI,
                             &tmpItem.Sale,
                             &tmpItem.Size,
                             &tmpItem.TotalPrice,
                             &tmpItem.NmId,
                             &tmpItem.Brand,
                             &tmpItem.Status)
            if err == nil {
                orderItems = append(orderItems,tmpItem)
            }
        }

        row := conn.QueryRow(context.Background(),
       `select  order_uid, track_number, entry, locale,
        internal_signature, customer_id, delivery_service,
        shardkey, sm_id, date_created, oof_shard, transaction,
        request_id, currency, provider, amount, payment_dt, bank,
        delivery_cost, goods_total, custom_fee, name_d, phone,
        zip, city, address, region, email
                                                                               
        from json_order as ord
            left join deliveries as del
                on ord.order_uid = del.order_uid_d
            left join payments as pay
                on ord.order_uid = pay.order_uid_p
            left join items as it
                on ord.order_uid = it.order_uid_i where ord.order_uid = $1;`,
        
        orderUids[i]) //conn.QueryRow

        if err != nil {
            fmt.Println("order error:",err)
            continue
        }

        err = row.Scan(&tmpOrder.OrderUid,
                       &tmpOrder.TrackNumber,
                       &tmpOrder.Entry,
                       &tmpOrder.Locale,
                       &tmpOrder.InternalSignature,
                       &tmpOrder.CustomerId,
                       &tmpOrder.DeliveryService,
                       &tmpOrder.Shardkey,
                       &tmpOrder.SmId,
                       &tmpOrder.DateCreated,
                       &tmpOrder.OofShard,
                       &tmpOrder.Payment.Transaction,
                       &tmpOrder.Payment.RequestId,
                       &tmpOrder.Payment.Currency,
                       &tmpOrder.Payment.Provider,
                       &tmpOrder.Payment.Amount,
                       &tmpOrder.Payment.PaymentDt,
                       &tmpOrder.Payment.Bank,
                       &tmpOrder.Payment.DeliveryCost,
                       &tmpOrder.Payment.GoodsTotal,
                       &tmpOrder.Payment.CustomFee,
                       &tmpOrder.Delivery.Name,
                       &tmpOrder.Delivery.Phone,
                       &tmpOrder.Delivery.Zip,
                       &tmpOrder.Delivery.City,
                       &tmpOrder.Delivery.Address,
                       &tmpOrder.Delivery.Region,
                       &tmpOrder.Delivery.Email)
        if err != nil {
            fmt.Println("db order error", err)
        }

        tmpOrder.Items = orderItems

        resJSON, err := json.MarshalIndent(tmpOrder,"","    ")
        if err != nil {
            fmt.Println(err)
        } else {
            cache[orderUids[i]] = resJSON
        }
    } //for i := range orderUids

    //starting hhtp server 
    go runHTTPserver();

    //opts := []nats.Option{nats.Name("L0WB")}
    nc, err := nats.Connect(stan.DefaultNatsURL)
	defer conn.Close(context.Background())

    if err != nil {
        fmt.Println(err)
    }
    defer nc.Close()

    sc, err := stan.Connect("test-cluster", 
                            "stan-sub", 
                            stan.NatsConn(nc),
                            stan.SetConnectionLostHandler(
                                func(_ stan.Conn, reason error) {
                                     fmt.Printf("Connection lost, reason: %v", reason)
                                }))
    if err != nil {
        fmt.Printf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, stan.DefaultNatsURL)
    }
 
    // Process Subscriber Options.
    startOpt := stan.StartAt(pb.StartPosition_NewOnly)
    //startOpt = stan.DeliverAllAvailable()

    subj := "ch"
    psqlw := func(msg *stan.Msg) {
        //write to psql
        psqlWrite(msg,conn)
    }

    sub, err := sc.QueueSubscribe(subj, "", psqlw, startOpt, stan.DurableName(""))
    if err != nil {
    sc.Close()
    }

    // Wait for a SIGINT (perhaps triggered by user with CTRL-C)
    // Run cleanup when signal is received
    signalChan := make(chan os.Signal, 1)
    cleanupDone := make(chan bool)
    signal.Notify(signalChan, os.Interrupt)

    go func() {
        for range signalChan {
            fmt.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n")
            sub.Unsubscribe()
            sc.Close()
            cleanupDone <- true
        }
    }()
    <-cleanupDone
}

func psqlWrite(m *stan.Msg,db *pgx.Conn) error {
    var order Order    
    err := json.Unmarshal(m.Data,&order)
    if err != nil {
        fmt.Println(err)
        return err
    }
    if order.OrderUid == "" {
        return err
    }
    fmt.Printf("Received msg!\nOrderUid: %s\n", order.OrderUid)

        _,err = db.Exec(context.Background(),`insert into deliveries(
                                                        order_uid_d,
                                                        name_d,
                                                        phone,
                                                        zip,
                                                        city,
                                                        address,
                                                        region,
                                                        email
                                                        ) values($1,$2,$3,$4,$5,$6,$7,$8)`,
                                                        order.OrderUid,
                                                        order.Delivery.Name,
                                                        order.Delivery.Phone,
                                                        order.Delivery.Zip,
                                                        order.Delivery.City,
                                                        order.Delivery.Address,
                                                        order.Delivery.Region,
                                                        order.Delivery.Email)
        if err != nil {
            fmt.Println("error dilevery:",err)
        }

        _,err = db.Exec(context.Background(),`insert into payments(
                                                        order_uid_p,
                                                        transaction,
                                                        request_id,
                                                        currency,
                                                        provider,
                                                        amount,
                                                        payment_dt,
                                                        bank,
                                                        delivery_cost,
                                                        goods_total,
                                                        custom_fee
                                                        ) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
                                                        order.OrderUid,
                                                        order.Payment.Transaction,
                                                        order.Payment.RequestId,
                                                        order.Payment.Currency,
                                                        order.Payment.Provider,
                                                        order.Payment.Amount,
                                                        order.Payment.PaymentDt,
                                                        order.Payment.Bank,
                                                        order.Payment.DeliveryCost,
                                                        order.Payment.GoodsTotal,
                                                        order.Payment.CustomFee)
        if err != nil {
            fmt.Println("error payment:",err)
        }

        _,err = db.Exec(context.Background(),`insert into items(
                                                        order_uid_i,
                                                        chrt_id,
                                                        track_number_i,
                                                        price,
                                                        rid,
                                                        name_i,
                                                        sale,
                                                        size,
                                                        total_price,
                                                        nm_id,
                                                        brand,
                                                        status
                                                        ) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
                                                        order.OrderUid,
                                                        order.Items[0].ChrtId,
                                                        order.Items[0].TrackNumberI,
                                                        order.Items[0].Price,
                                                        order.Items[0].Rid,
                                                        order.Items[0].NameI,
                                                        order.Items[0].Sale,
                                                        order.Items[0].Size,
                                                        order.Items[0].TotalPrice,
                                                        order.Items[0].NmId,
                                                        order.Items[0].Brand,
                                                        order.Items[0].Status)

        if err != nil {
            fmt.Println("error items:",err);
        }

        _,err = db.Exec(context.Background(),`insert into json_order(
                                                        order_uid,
                                                        track_number,
                                                        entry,
                                                        delivery,
                                                        payment,
                                                        items,
                                                        locale,
                                                        internal_signature,
                                                        customer_id,
                                                        delivery_service,
                                                        shardkey,
                                                        sm_id,
                                                        date_created,
                                                        oof_shard
                                                        ) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, 
                                                        order.OrderUid,
                                                        order.TrackNumber,
                                                        order.Entry,
                                                        order.OrderUid,
                                                        order.OrderUid,
                                                        order.OrderUid,
                                                        order.Locale,
                                                        order.InternalSignature,
                                                        order.CustomerId,
                                                        order.DeliveryService,
                                                        order.Shardkey,
                                                        order.SmId,
                                                        order.DateCreated,
                                                        order.OofShard)
        if err != nil {
            fmt.Println("error order:",err)
        }

    return nil
}

func runHTTPserver() {
    http.HandleFunc("/list",listHandler)
    http.HandleFunc("/",handler)
    http.ListenAndServe("localhost:8000", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
    id,ok := r.URL.Query()["id"]

    if !ok {
        fmt.Fprintf(w,"Wrong request!\n")
    } else {
        if cache[id[0]] != nil {
            fmt.Fprintf(w,"%s\n",cache[id[0]])
        } else {
            fmt.Fprintf(w,"there is no order with id %s\n",id[0])
        }
    }
}

func listHandler(w http.ResponseWriter, r *http.Request) {
    var res []byte
    for i := range cache {
        res = append(res, i...)
        res = append(res, '\n')
    }
    fmt.Fprintf(w, "%s \n", res)
}













