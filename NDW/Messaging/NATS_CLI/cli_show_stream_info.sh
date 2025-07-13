
echo
echo "--------------------"
nats stream info NEWS

echo
echo "--------------------"
echo "Total Consumption:"
nats stream info NEWS | awk '
/Maximum Bytes:/ {
  value = $3;
  unit = $4;
  if (unit == "MiB") bytes = value * 1048576;
  else if (unit == "GiB") bytes = value * 1073741824;
  else bytes = value;  # assume raw bytes
  gb = bytes / 1000000000;
  printf "Maximum Bytes: %.6f GB\n", gb;
}
/^ *Bytes:/ {
  value = $2;
  unit = $3;
  if (unit == "MiB") bytes = value * 1048576;
  else if (unit == "GiB") bytes = value * 1073741824;
  else bytes = value;
  gb = bytes / 1000000000;
  printf "Used Bytes:    %.6f GB\n", gb;
}'

echo "--------------------"

#echo
#echo "--------------------"
#nats server report jetstream
#echo "--------------------"
#echo

