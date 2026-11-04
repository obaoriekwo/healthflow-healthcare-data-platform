{#
    Fails for rows where this column is earlier than other_column
    (for example a discharge before its admission). NULLs pass.
#}
{% test not_before(model, column_name, other_column) %}

select {{ column_name }}, {{ other_column }}
from {{ model }}
where {{ column_name }} < {{ other_column }}

{% endtest %}
