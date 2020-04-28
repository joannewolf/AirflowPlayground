echo "{{ ti.xcom_pull(key="k1") }}" "{{ ti.xcom_pull(key="k1-2") }}" "{{ ti.xcom_pull("push_bash2") }}"
echo "first argument: $1"