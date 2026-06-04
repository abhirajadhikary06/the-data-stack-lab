{% macro payment_types(payment_method) %}
    case
        when {{ payment_method }} = 'Card' then 'Credit/Debit Card'
        when {{ payment_method }} = 'UPI' then 'Digital Wallet'
        when {{ payment_method }} = 'NetBanking' then 'Bank Transfer'
        else 'Other'
    end
{% endmacro %}