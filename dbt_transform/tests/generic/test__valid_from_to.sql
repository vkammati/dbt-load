{% test test__valid_from_to(model) %}

    {#
     This test is here to demonstrate how to write a generic, table level test. Since the
     valid_from and valid_to column are created by a macro and fully depend on the code
     of that macro (not the data input), this check is also covered by a unit test.
    #}

    select
        *
    from {{ model }}
    where valid_from > valid_to

{% endtest %}
