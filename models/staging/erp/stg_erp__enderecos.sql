with 

source as (

    select * from {{ source('erp', 'Address') }}

),

enderecos as (

    select
        addressid,
        addressline1,
        addressline2,
        city,
        stateprovinceid,
        postalcode,
        spatiallocation,
        rowguid,
        modifieddate

    from source

)

select * from enderecos
