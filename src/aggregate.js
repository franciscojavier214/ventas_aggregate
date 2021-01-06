// importe total de la venta. (precio_unitario * número_unidades + coste_transporte)
db.v2.aggregate(
    [
      { $project: {  _id : 0 , artículo: 1, importe_total: {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] } } }
    ]
 )
/*
{ "artículo" : "Lavadora AEG L6FBI824U", "importe_total" : 1230 }
{ "artículo" : "Campana extractora Orbegozo DS 56190", "importe_total" : 1470 }
{ "artículo" : "Horno eléctrico Candy FCS100X/E", "importe_total" : 1840 }
{ "artículo" : "Microondas LG MH6535GDS", "importe_total" : 1240 }
{ "artículo" : "Placa de inducción Siemens EH651FDC1E", "importe_total" : 5030 }
{ "artículo" : "Congelador CHiQ FCF197D", "importe_total" : 4220 }
{ "artículo" : "Frigorífico Balay 3FIB3420", "importe_total" : 4550 }
{ "artículo" : "Frigorífico Samsung RB29HER2CSA/EF", "importe_total" : 8520 }
{ "artículo" : "Horno eléctrico Siemens HB673GBS1", "importe_total" : 4020 }
{ "artículo" : "Aire Acondicionado MitsubishiSplit MSZ-HR35VF", "importe_total" : 8040 }
{ "artículo" : "Lavadora Sauber WM6129", "importe_total" : 3290 }
{ "artículo" : "Microondas LG MH6535GDS", "importe_total" : 940 }
{ "artículo" : "Aire acondicionado TERMOTEK AIRPLUS C9", "importe_total" : 3440 }
{ "artículo" : "Microondas Cecotec ProClean 3040", "importe_total" : 750 }
{ "artículo" : "Frigorífico Beko RSNE445I31XBN", "importe_total" : 6930 }
*/

// beneficios. (importe total - coste total )
db.v2.aggregate(
    [
        { $project:
            {
                _id : 0 ,
                artículo: 1,
                número_unidades: 1,
                beneficios: {$subtract:
                [
                    {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                    {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                ]}
            } 
        }
    ]   
)
/*
{ "artículo" : "Lavadora AEG L6FBI824U", "número_unidades" : 3, "beneficios" : 705 }
{ "artículo" : "Campana extractora Orbegozo DS 56190", "número_unidades" : 8, "beneficios" : 924 }
{ "artículo" : "Horno eléctrico Candy FCS100X/E", "número_unidades" : 12, "beneficios" : 800 }    
{ "artículo" : "Microondas LG MH6535GDS", "número_unidades" : 8, "beneficios" : 440 }
{ "artículo" : "Placa de inducción Siemens EH651FDC1E", "número_unidades" : 10, "beneficios" : 1310 }
{ "artículo" : "Congelador CHiQ FCF197D", "número_unidades" : 16, "beneficios" : 1784 }
{ "artículo" : "Frigorífico Balay 3FIB3420", "número_unidades" : 8, "beneficios" : 1900 }
{ "artículo" : "Frigorífico Samsung RB29HER2CSA/EF", "número_unidades" : 16, "beneficios" : 2960 }
{ "artículo" : "Horno eléctrico Siemens HB673GBS1", "número_unidades" : 6, "beneficios" : 1740 }
{ "artículo" : "Aire Acondicionado MitsubishiSplit MSZ-HR35VF", "número_unidades" : 14, "beneficios" : 2640 }
{ "artículo" : "Lavadora Sauber WM6129", "número_unidades" : 12, "beneficios" : 1300 }
{ "artículo" : "Microondas LG MH6535GDS", "número_unidades" : 6, "beneficios" : 500 }
{ "artículo" : "Aire acondicionado TERMOTEK AIRPLUS C9", "número_unidades" : 10, "beneficios" : 1480 }
{ "artículo" : "Microondas Cecotec ProClean 3040", "número_unidades" : 12, "beneficios" : 300 }
{ "artículo" : "Frigorífico Beko RSNE445I31XBN", "número_unidades" : 16, "beneficios" : 3300 }
*/

// mejores clientes usando sort y limit ( agrupados por el campo clientes, se suma el beneficio de todos sus pedidos).
db.v2.aggregate(
    [
        { $group:
            {
                _id: "$cliente",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                         {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                        ]
                    }
                }
            }
        },{$sort: {beneficio_total:-1} }, {$limit:5} 
    ]
)
/*
{ "_id" : "Cocinas Linero", "beneficio_total" : 10110 }
{ "_id" : "Electrodomésticos Antonio Domínguez", "beneficio_total" : 4064 }
{ "_id" : "Factory Outlet Gonzalez Infantes", "beneficio_total" : 3704 }
{ "_id" : "Electro Factory's Ruiz", "beneficio_total" : 3200 }
{ "_id" : "ELECTROTELE GOMEZ S.L", "beneficio_total" : 705 }
*/

// mejores vendedores (similar a la anterior pero sustituyendo el campo).
db.v2.aggregate(
    [
        { $group:
            {

                _id: "$vendedor",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                         {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                        ]
                    }
                }
            }
        },{$sort: {beneficio_total:-1} }, {$limit:5} 
    ]
)
/*
{ "_id" : "Suministros Climastock", "beneficio_total" : 7804 }
{ "_id" : "Reverse Container", "beneficio_total" : 6199 }
{ "_id" : "Herrajes Andalucía", "beneficio_total" : 5400 }
{ "_id" : "Equipamiento de Hostelería J. Rafael Cámara", "beneficio_total" : 1740 }
{ "_id" : "Frigeria Hostelería", "beneficio_total" : 940 }
*/

// mejores artículos (similar a la anterior pero sustituyendo el campo).
db.v2.aggregate(
    [
        { $group:
            {
                _id: "$artículo",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                         {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                        ]
                    }
                }
            }
        },
        { $match:
            {
                cast:{$ne:"Undefined"}
            }
        },
        {$sort: {beneficio_total:-1} }, {$limit:5}
    ]
)
/*
{ "_id" : "Frigorífico Beko RSNE445I31XBN", "beneficio_total" : 3300 }
{ "_id" : "Frigorífico Samsung RB29HER2CSA/EF", "beneficio_total" : 2960 }
{ "_id" : "Aire Acondicionado MitsubishiSplit MSZ-HR35VF", "beneficio_total" : 2640 }
{ "_id" : "Frigorífico Balay 3FIB3420", "beneficio_total" : 1900 }
{ "_id" : "Congelador CHiQ FCF197D", "beneficio_total" : 1784 }
*/

// mejores meses (ademas de especificar el campo fecha_venta aclaramos que solo usaremos los meses).
db.v2.aggregate(
    [
        { $group:
            {
                _id: {$month:("$fecha_venta")} ,
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                         {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                        ]
                    }
                }
            }
        },
        {$sort: {beneficio_total:-1} }
    ]
)
/*
{ "_id" : 3, "beneficio_total" : 6600 }
{ "_id" : 5, "beneficio_total" : 5080 }
{ "_id" : 4, "beneficio_total" : 4440 }
{ "_id" : 2, "beneficio_total" : 3094 }
{ "_id" : 1, "beneficio_total" : 2869 }
*/

// los beneficios de cada día ordenados segun su fecha, util para verificar los resultados de la consulta anterior.
db.v2.aggregate(
    [
        { $project:
            {
                _id: 0,
                fecha_venta: 1,
                beneficios: {$subtract:
                [
                    {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                    {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                ]}
            } 
        },{$sort: {fecha_venta:-1}}
    ]   
)
/*
{ "fecha_venta" : ISODate("2020-05-22T00:00:00Z"), "beneficios" : 3300 }
{ "fecha_venta" : ISODate("2020-05-12T00:00:00Z"), "beneficios" : 300 }
{ "fecha_venta" : ISODate("2020-05-05T00:00:00Z"), "beneficios" : 1480 }
{ "fecha_venta" : ISODate("2020-04-28T00:00:00Z"), "beneficios" : 500 }
{ "fecha_venta" : ISODate("2020-04-20T00:00:00Z"), "beneficios" : 1300 }
{ "fecha_venta" : ISODate("2020-04-12T00:00:00Z"), "beneficios" : 2640 }
{ "fecha_venta" : ISODate("2020-03-27T00:00:00Z"), "beneficios" : 1740 }
{ "fecha_venta" : ISODate("2020-03-13T00:00:00Z"), "beneficios" : 2960 }
{ "fecha_venta" : ISODate("2020-03-02T00:00:00Z"), "beneficios" : 1900 }
{ "fecha_venta" : ISODate("2020-02-20T00:00:00Z"), "beneficios" : 1784 }
{ "fecha_venta" : ISODate("2020-02-09T00:00:00Z"), "beneficios" : 1310 }
{ "fecha_venta" : ISODate("2020-01-30T00:00:00Z"), "beneficios" : 440 }
{ "fecha_venta" : ISODate("2020-01-23T00:00:00Z"), "beneficios" : 800 }
{ "fecha_venta" : ISODate("2020-01-15T00:00:00Z"), "beneficios" : 924 }
{ "fecha_venta" : ISODate("2020-01-05T00:00:00Z"), "beneficios" : 705 }
*/

// beneficio promedio mensual del año.
db.v2.aggregate(
    [
        { $group:
            {
                _id: {$month:("$fecha_venta")} ,
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] },
                         {$subtract: [ {$multiply: ["$precio_unitario_por_mayor", "$número_unidades" ]}, "$coste_transporte" ] }
                        ]
                    }
                }
            }
        },
        {$group:
            {
                _id:{$year:("$fecha_venta")},
                beneficio_promedio_mensual: {$avg: "$beneficio_total" }
            }
        }
    ]
)
//la id correspondiente al año se muestra como null pero el promedio es correcto.
//{ "_id" : null, "beneficio_promedio" : 4416.6 } 

// buscaremos el clientes que ha realizado la mayor compra.
db.v2.aggregate(
    [
        { $group:
            {
                _id: "$cliente",
                mayor_compra: { $max: {$add: [ {$multiply: ["$precio_unitario", "$número_unidades" ]}, "$coste_transporte" ] } }
            }
        },{$sort:{mayor_compra:-1}}
    ]
)
/*
{ "_id" : "Cocinas Linero", "mayor_compra" : 8520 }
{ "_id" : "Electrodomésticos Antonio Domínguez", "mayor_compra" : 8040 }
{ "_id" : "Electro Factory's Ruiz", "mayor_compra" : 4550 }
{ "_id" : "Factory Outlet Gonzalez Infantes", "mayor_compra" : 4220 }
{ "_id" : "ELECTROTELE GOMEZ S.L", "mayor_compra" : 1230 }
{ "_id" : "Pixeloy", "mayor_compra" : 750 }
*/

// porcentaje de beneficio por articulo (precio_unitario_por_mayor/precio_unitario * 100 = % )
db.v2.aggregate(
    [
        { $project:
            {
                _id : 0 ,
                artículo: 1,
                porcentaje_beneficio:{
                    $multiply: [ {$divide: [ "$precio_unitario_por_mayor", "$precio_unitario" ]} , 100 ]   
                }
            } 
        }
    ]   
)
/*
{ "artículo" : "Lavadora AEG L6FBI824U", "porcentaje_beneficio" : 50 }
{ "artículo" : "Campana extractora Orbegozo DS 56190", "porcentaje_beneficio" : 40 }
{ "artículo" : "Horno eléctrico Candy FCS100X/E", "porcentaje_beneficio" : 60 }
{ "artículo" : "Microondas LG MH6535GDS", "porcentaje_beneficio" : 70 }
{ "artículo" : "Placa de inducción Siemens EH651FDC1E", "porcentaje_beneficio" : 75 }
{ "artículo" : "Congelador CHiQ FCF197D", "porcentaje_beneficio" : 60 }
{ "artículo" : "Frigorífico Balay 3FIB3420", "porcentaje_beneficio" : 60.71428571428571 }
{ "artículo" : "Frigorífico Samsung RB29HER2CSA/EF", "porcentaje_beneficio" : 66.0377358490566 }
{ "artículo" : "Horno eléctrico Siemens HB673GBS1", "porcentaje_beneficio" : 61.53846153846154 }
{ "artículo" : "Aire Acondicionado MitsubishiSplit MSZ-HR35VF", "porcentaje_beneficio" : 68.42105263157895 }
{ "artículo" : "Lavadora Sauber WM6129", "porcentaje_beneficio" : 62.96296296296296 }
{ "artículo" : "Microondas LG MH6535GDS", "porcentaje_beneficio" : 53.333333333333336 }
{ "artículo" : "Aire acondicionado TERMOTEK AIRPLUS C9", "porcentaje_beneficio" : 58.82352941176471 }
{ "artículo" : "Microondas Cecotec ProClean 3040", "porcentaje_beneficio" : 66.66666666666666 }
{ "artículo" : "Frigorífico Beko RSNE445I31XBN", "porcentaje_beneficio" : 53.48837209302325 }
*/