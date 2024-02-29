/*
    Este test verifica que los valores en la columna del modelo escogido sean positivos.
*/

{% test positive_values(model, column_name) %}


   select *
   from {{ model }}
   where {{ column_name }} < 0


{% endtest %}
