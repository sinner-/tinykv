# tinykv
Tiny K/V store in golang

## Example usage
```
$ telnet 127.0.0.1 9999
Trying 127.0.0.1...
Connected to 127.0.0.1.
Escape character is '^]'.
PUT somekey someval   
PUT someotherkey someotherval
PUT foo bar
GET foo
bar
LIST
someotherkey: someotherval
foo: bar
somekey: someval
DEL somekey
LIST
someotherkey: someotherval
foo: bar
^]
telnet> Connection closed.
```
