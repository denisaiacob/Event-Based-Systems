# Event-Based-Systems


## **Specificatii**
### **Fără Paralelizare:**
**Numărul de Publicatii Generate:** 1.000.000

**Timpul de Execuție:** 5.8003 seconds

**Numărul de Subscriptii Generate:** 1.000.000

**Timpul de Execuție:** 9.1085 seconds



### **Cu Paralelizare de tip multiprocessing:**

**Numarul de procese:** 4

**Numărul de Publicatii Generate:** 1.000.000

**Timpul de Execuție:** 3.1604 seconds

**Numărul de Subscriptii Generate:** 1.000.000

**Timpul de Execuție:** 5.2665 seconds



### Specificațiile Procesorului: 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz   2.80 GHz


## **Descriere**

Scrieti un program care sa genereze aleator seturi echilibrate de subscriptii si publicatii cu posibilitatea de fixare a: numarului total de mesaje (publicatii, respectiv subscriptii), ponderii pe frecventa campurilor din subscriptii si ponderii operatorilor de egalitate din subscriptii pentru cel putin un camp. Publicatiile vor avea o structura fixa de campuri. Implementarea temei va include o posibilitate de paralelizare pentru eficientizarea generarii subscriptiilor si publicatiilor, si o evaluare a timpilor obtinuti.

Exemplu:
**Publicatie:** {(stationid,1);(city,"Bucharest");(temp,15);(rain,0.5);(wind,12);(direction,"NE");(date,2.02.2023)} - Structura fixa a campurilor publicatiei e: stationid-integer, city-string, temp-integer, rain-double, wind-integer, direction-string, date-data; pentru anumite campuri (stationid, city, direction, date), se pot folosi seturi de valori prestabilite de unde se va alege una la intamplare; pentru celelalte campuri se pot stabili limite inferioare si superioare intre care se va alege una la intamplare.

**Subscriptie:** {(city,=,"Bucharest");(temp,>=,10);(wind,<,11)} - Unele campuri pot lipsi; frecventa campurilor prezente trebuie sa fie configurabila (ex. 90% city - exact 90% din subscriptiile generate, cu eventuala rotunjire la valoarea cea mai apropiata de procentul respectiv, trebuie sa includa campul "city"); pentru cel putin un camp (exemplu - city) trebui sa se poate configura un minim de frecventa pentru operatorul "=" (ex. macar 70% din subscriptiile generate sa aiba ca operator pe acest camp egalitatea).


