rdkafka-hang: rdkafka-hang.c
	$(CC) \
		-I../librdkafka/src \
		-L../librdkafka/src \
		-lrdkafka \
		-g \
		-o rdkafka-hang \
		rdkafka-hang.c
