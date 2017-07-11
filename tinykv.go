package main

import (
    "net"
    "fmt"
    "log"
    "bufio"
    "strings"
    "sync"
    "io"
)

var (
    kvStore = make(map[string]string)
    lock = sync.RWMutex{}
)

func kvGet(k string, conn net.Conn) string {
    lock.RLock()
    defer lock.RUnlock()

    return kvStore[k]
}

func kvList(w io.Writer) error {
    lock.RLock()
    defer lock.RUnlock()

    for k, v := range kvStore {
        _, err := w.Write([]byte(fmt.Sprintf("%s: %s\n", k, v)))
        if err != nil {
            return err
        }
    }

    return nil
}

func kvSet(k string, v string) {
    lock.Lock()
    defer lock.Unlock()

    if v == "" {
        delete(kvStore, k)
    } else {
        kvStore[k] = v
    }
}

func handleClient(conn net.Conn) {
    defer conn.Close()

    reader := bufio.NewReader(conn)

    for {
        message, err := reader.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                log.Printf("Client %s disconnected.", conn.RemoteAddr())
            } else {
                log.Printf("Read error from client %s - ", conn.RemoteAddr(), err.Error())
            }
            return
        }

        message = strings.TrimSpace(message)
        log.Printf("%s - %s", conn.RemoteAddr(), message)
        messageComponents := strings.Split(message, " ")
        switch messageComponents[0] {
            case "PUT":
                if len(messageComponents) != 3 {
                    log.Printf("Invalid command %s from %s.", message, conn.RemoteAddr())
                    return
                }
                kvSet(messageComponents[1], messageComponents[2])
            case "GET":
                if len(messageComponents) != 2 {
                    log.Printf("Invalid command %s from %s.", message, conn.RemoteAddr())
                    return
                }
                _, err = conn.Write([]byte(fmt.Sprintf("%s\n", kvStore[messageComponents[1]])))
                if err != nil {
                    log.Print("Error sending response.")
                }
           case "DEL":
                if len(messageComponents) != 2 {
                    log.Printf("Invalid command %s from %s.", message, conn.RemoteAddr())
                    return
                }
                kvSet(messageComponents[1], "")
           case "LIST":
                if len(messageComponents) != 1 {
                    log.Printf("Invalid command %s from %s.", message, conn.RemoteAddr())
                    return
                }
                err = kvList(conn)
                if err != nil {
                    log.Print("Error sending response.")
                }
           default:
               log.Printf("Invalid command %s from %s.", message, conn.RemoteAddr())
               return
        }
    }
}

func main() {
    listener, err := net.Listen("tcp", "127.0.0.1:9999")
    if err != nil {
        log.Fatal(err.Error())
    }

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Print(err.Error())
            continue
        }

        log.Print("New connection from: ", conn.RemoteAddr())
        go handleClient(conn)
    }
}
