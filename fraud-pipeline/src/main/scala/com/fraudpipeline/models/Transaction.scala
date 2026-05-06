package com.fraudpipeline.models

case class Transaction(
  transactionId: String,
  accountId: String,
  amount: Double,
  timestamp: String, // ISO-8601 string
  merchantId: String,
  location: String
)
