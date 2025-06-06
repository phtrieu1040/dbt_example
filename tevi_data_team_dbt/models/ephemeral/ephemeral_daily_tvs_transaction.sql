select
    toDate(t.created_at) as transaction_date,
    t.created_at as transaction_datetime,
    t.order_id,
    t.user_id,
    toString(t.id) as consum_transaction_id, -- from user to creator
    sum(t.amount / -100) as usd_amount
from tevi_billy.billing_tvstransaction as t
group by all