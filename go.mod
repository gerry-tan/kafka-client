module kafka-client

go 1.12

replace (
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20190404164418-38d8ce5564a5
	golang.org/x/net => github.com/golang/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys => github.com/golang/sys v0.0.0-20190405154228-4b34438f7a67
	golang.org/x/text => github.com/golang/text v0.3.0
)

require (
	github.com/Shopify/sarama v1.22.0
	github.com/bsm/sarama-cluster v2.1.15+incompatible
)
