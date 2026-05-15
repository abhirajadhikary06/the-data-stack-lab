### Schema Building (diagrams.io)
```
Table checkout_transactions {
  checkout_id integer [primary key]
  order_id integer
  customer_id integer
  amount real
  currency text
  payment_method text
  payment_status text
  gateway_transaction_id varchar
  created_at timestamp
  updated_at timestamp
}

Table checkout_audit {
  audit_id integer [primary key]
  checkout_id integer
  old_status text
  new_status text
  failure_reason text
  refund_status text
  risk_score real
  event_time timestamp
  metadata json
}

Ref transaction_audit: checkout_transactions.checkout_id < checkout_audit.checkout_id
```