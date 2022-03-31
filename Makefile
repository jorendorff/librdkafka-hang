rdkafka-hang: rdkafka-hang.c
	clang -I/usr/local/Cellar/librdkafka/1.8.2/include -L/usr/local/Cellar/librdkafka/1.8.2/lib -lrdkafka \
		-g \
		-o rdkafka-hang \
		rdkafka-hang.c
