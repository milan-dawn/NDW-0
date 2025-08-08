# NATS_CLI — JetStream Helper Scripts

Convenience shell scripts for common JetStream operations when testing NDW.

## Examples

- Create stream:

```sh
./CreateNewsStream.sh
```

- List stream/consumer info:

```sh
./cli_show_stream_info.sh
./cli_list_push_consumer_info.sh
./cli_list_pull_consumer_info.sh
```

- Send messages:

```sh
./cli_send_push_message.sh
./cli_send_pull_messages.sh
```

- Pop messages from consumers:

```sh
./cli_pop_messages_push_consumers.sh
./cli_pop_messages_pull_consumers.sh
```

Adjust scripts and JSON files (e.g., `NEWS.JSON`) to match your environment.