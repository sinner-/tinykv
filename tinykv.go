package main

import (
    "net"
    "fmt"
    "log"
    "bufio"
    "regexp"
    "strings"
)

var commandPattern = regexp.MustCompile("^(PUT|GET|DEL|LIST)")
var putPattern = regexp.MustCompile("^PUT [\x00-\x7F]{1,255} [\x00-\x7F]{1,255}$")
var getPattern = regexp.MustCompile("^GET [\x00-\x7F]{1,255}$")
var delPattern = regexp.MustCompile("^DEL [\x00-\x7F]{1,255}$")
var listPattern = regexp.MustCompile("^LIST$")
var kvStore = make(map[string]string)

func handleClient(conn net.Conn) {
    for {
        message, err := bufio.NewReader(conn).ReadString('\n')
        if err != nil {
            log.Print(err.Error())
            conn.Close()
            break
        }

        message = strings.TrimSpace(message)
        if !commandPattern.MatchString(message) {
            log.Print(fmt.Sprintf("Invalid command: %s.", message))
            conn.Close()
            break
        }

        messageComponents := strings.Split(message, " ")
        switch messageComponents[0] {
            case "PUT":
                if !putPattern.MatchString(message) {
                    log.Print(fmt.Sprintf("Invalid command: %s.", message))
                    conn.Close()
                    break
                }

                kvStore[messageComponents[1]] = messageComponents[2]
            case "GET":
                if !getPattern.MatchString(message) {
                    log.Print(fmt.Sprintf("Invalid command: %s.", message))
                    conn.Close()
                    break
                }
                conn.Write([]byte(fmt.Sprintf("%s\n", kvStore[messageComponents[1]])))
           case "DEL":
                if !delPattern.MatchString(message) {
                    log.Print(fmt.Sprintf("Invalid command: %s.", message))
                    conn.Close()
                    break
                }
                delete(kvStore, messageComponents[1])
           case "LIST":
                if !listPattern.MatchString(message) {
                    log.Print(fmt.Sprintf("Invalid command: %s.", message))
                    conn.Close()
                    break
                }
                for k, v := range kvStore {
                    conn.Write([]byte(fmt.Sprintf("%s: %s\n", k, v)))
                }
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

        go handleClient(conn)
    }
}
