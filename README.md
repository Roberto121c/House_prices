# HOUSE PRICES
### Project overview
* Se hace un proceso de ETL completo usando diferentes tecnologias (Pyspark, beatifulsoup, AWS, sklearn)
* Se extrae informacion de una pagina de ventas de inmuebles, los datos se suben crudos a S3
* Con Glue y pyspark se eliminan duplicados, se da formato a los campos y se almacena en athena sql
* Finalmente se hace EDA y se usa regresion lineal para calcular los valores faltantes del area del inmueble

### Objectives:
Se puede observar un ETL completo desde obtener datos crudos hasta obtener conocimiento de estos datos

### Part 1: Extraction 
- La extraccion se hace de la pagina **[www.vivanuncios.com.mx](https://www.vivanuncios.com.mx/s-venta-inmuebles/estado-de-mexico/v1c1097l1014p1)**
- En esta parte se extrae la informacion usando BeautifulSoup y se usa geopy para calcular las coordenadas
- You can see the full and detailed script **[HERE](https://github.com/Roberto121c/House_prices/blob/main/Code/House_pricing_1.ipynb)**:
![Imagenesl](/images/house_prices_1_1.PNG)

[![](images/Sales query.PNG)](https://github.com/Roberto121c/Sales_Management/tree/main/Query)

### Part 2: Transformation and Load
Cual es el objetivo de esta parte
Que se hizo
Que herramientas se utilizaron
You can see all the script **[HERE](https://github.com/Roberto121c/Sales_Management/tree/main/Query)**:
![Imagenesl](images/Data model.png)

### Part 3: Descriptive and predictive analysis
Cual es el objetivo de esta parte
Que se hizo
Que herramientas se utilizaron
You can see all the script **[HERE](https://github.com/Roberto121c/Sales_Management/tree/main/Query)**:
![Imagenesl](images/Data model.png)

### Conclusions
* Que se logro?
* Que herramientas se usaron
* De que sirve


