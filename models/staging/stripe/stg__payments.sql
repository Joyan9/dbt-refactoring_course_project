SELECT * FROM {{ source('jaffle_shop', 'stripe_payments') }}