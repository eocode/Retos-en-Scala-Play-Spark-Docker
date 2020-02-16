
# Apuntes SPARK <!-- omit in toc -->

Apuntes sobre Spark, Hadoop y Scala

## Tabla de Contenido<!-- omit in toc -->
- [Big Data](#big-data)
  - [Hadoop](#hadoop)
  - [Apache Spark](#apache-spark)
    - [Componentes de Spark](#componentes-de-spark)
    - [Tipos de instalación de Hadoop](#tipos-de-instalaci%c3%b3n-de-hadoop)
    - [Lenguajes de programación](#lenguajes-de-programaci%c3%b3n)
- [Programación funcional con Scala](#programaci%c3%b3n-funcional-con-scala)
  - [Tipos de dato](#tipos-de-dato)
  - [Expresiones](#expresiones)
  - [Funciones](#funciones)
    - [Lambdas](#lambdas)
    - [Objetos](#objetos)
    - [Función de funciones](#funci%c3%b3n-de-funciones)
    - [Métodos](#m%c3%a9todos)
      - [Reto](#reto)
        - [Solución](#soluci%c3%b3n)
  - [Colecciones](#colecciones)
    - [Secuencias, conjuntos y mapas](#secuencias-conjuntos-y-mapas)
      - [Secuencias](#secuencias)
      - [Conjuntos](#conjuntos)
      - [Mapas](#mapas)
      - [Recorrer colecciones](#recorrer-colecciones)
      - [Reto](#reto-1)
  - [Tuplas y Objetos](#tuplas-y-objetos)
    - [Lens](#lens)
    - [Pattern Machine](#pattern-machine)
      - [Reto](#reto-2)
  - [Tail Recursion](#tail-recursion)
  - [Agregaciones](#agregaciones)
    - [FoldLeft y FoldRight](#foldleft-y-foldright)
  - [Conceptos teoricos de la programación funcional](#conceptos-teoricos-de-la-programaci%c3%b3n-funcional)
    - [Funciones](#funciones-1)
      - [Ejemplos](#ejemplos)
    - [Razonamiento inductivo](#razonamiento-inductivo)
    - [Razonamiento con tipos](#razonamiento-con-tipos)
      - [Tipados vs Dinámicos](#tipados-vs-din%c3%a1micos)
      - [Sistema de tipos](#sistema-de-tipos)
      - [Lenguage ML](#lenguage-ml)
      - [Proposiciones y pruebas](#proposiciones-y-pruebas)
      - [Tipos vs Strings](#tipos-vs-strings)
      - [Razonamiento de tipos](#razonamiento-de-tipos)
      - [Traits](#traits)
      - [Tipos genericos](#tipos-genericos)
      - [Tipos de datos algebraicos](#tipos-de-datos-algebraicos)
      - [Evaluación peresoza o Lazy](#evaluaci%c3%b3n-peresoza-o-lazy)
      - [Disyunciones](#disyunciones)
        - [Option](#option)
        - [Try](#try)
        - [Either](#either)
- [Programación con SPARK](#programaci%c3%b3n-con-spark)
  - [RDD](#rdd)
    - [SparkConf y SparkContext](#sparkconf-y-sparkcontext)


# Big Data
* <b>Volumen:</b> Grandes cantidades de datos
* <b>Variedad:</b> Ser capaces de trabajar con tipos de datos muy diversos.
* <b>Velocidad:</b> Se refiere al tiempo utilizado en la ingesta, transformación y utilización de estos datos.
* <b>Valor:</b> Transformar los datos en información útil.
* <b>Veracidad: </b> Como de fiable es la información recibida.

## Hadoop
Es un framework que permite el procesamiento distribuido de grandes cantidad de datos, usando modelos de programación simple en un clúster. <b>Procesa en Disco.</b>

* Procesamiento distribuido
* Eficiente
* Económico (Trabaja en disco HDFS)
* Escalable
* Tolerante a fallos
* Open Source

## Apache Spark
Apache Spark es un framework de computación en clúster. Apache Spark es un sistema de computación que se basa en Hadoop Map Reduce y que, principalmente, <b>permite dividir o paralelizar el trabajo</b>, ya que normalmente se instala en un clúster de máquina. La idea es que tengamos n máquinas, por ejemplo diez máquinas, y cada una de esas instancias va a tener instalada una versión de Apache Spark. <b>Procesa en memoria</b>

`
// Hadoop particiona en disco con HDFS y Spark lo optimiza en memoria, se debe tener en cuenta a la hora de hacer los algoritmos para un mejor rendimiento
`

Descarga de paquetes
https://archive.apache.org/dist/spark/

### Componentes de Spark
 * <b>Spark Core:</b> Es la base o conjunto de librerías donde se apoya el resto de módulos. Es el núcleo del framework.
 * <b>Spark SQL:</b> Es el módulo para el procesamiento de datos estructurados y semi-estructurados. Con este módulo vamos a poder transformar y realizar operaciones sobre los RDD o los dataframes. Está pensado exclusivamente para el tratamiento de los datos.
 * <b>Spark Streaming:</b> Es el que permite la ingesta de datos en tiempo real. Si tenemos una fuente, por ejemplo Kafka o Twitter, con este módulo podemos ingestar los datos de esa fuente y volcarlos a un destino. Entre la ingesta de datos y su volcado posterior, podemos tener una serie de transformaciones.
 * <b>Spark MLLib:</b> Es una librería muy completa que contiene numerosos algoritmos de Machine Learning, tanto de clusterización, clasificación, regresión, etc. Nos permite, de una forma amigable, poder utilizar algoritmos de Machine Learning.
 * <b>Spark Graph:</b> Permite el procesamiento de grafos (DAG). No permite pintar grafos, sino que permite crear operaciones con grafos, con sus nodos y aristas, e ir realizando operaciones.

### Tipos de instalación de Hadoop
 * <b>Modo Standalone:</b> Tenemos el HDFS (Hadoop Distributed File System), es decir, el sistema de archivos distribuido de Haddop, y por encima estaría Apache Spark.
 * <b>Hadoop V1 (SIMR):</b>  Tenemos el HDFS y por encima Map Reduce, y por encima del mismo estaría Apache Spark.
 * <b>Hadoop V2 (YARN):</b> Tenemos nuestro HDFS y por encima un gestor de recursos, como puede ser YARN o Mesos, y por encima del mismo estaría Apache Spark. Ese gestor de recursos se va a encargar de gestionar todos los recursos de nuestro clúster.

### Lenguajes de programación
* R: Bueno para investigación, graficación y análisis de datos.
* Python: Bueno para proyectos de baja/mediana escala, construir modelos y análizar datos, para startups y equipos pequeños.
* Scala/Java: Bueno para programación robusta con equipos grandes y muchos programadores; tiene menos utilidades de machine learning que R y Python, pero lo compensa con una mejor mantenibilidad de los proyectos y rendimiento.

# Programación funcional con Scala

* Funcional - Todo es una expresión
* Pensado para programarse en una línea y encadenarse
* Sintaxis simplificada
* Evita los datos mutables
* Staticamente tipado

## Tipos de dato
[Tipos de dato - documentación](https://docs.scala-lang.org/tour/unified-types.html#nothing-and-null)

## Expresiones
```scala
def x = 3
if(x!=3) "No es 3" else  "es 3"
if(x!=3) "No es 3"
val u = ()
```

## Funciones
`f: D->R`
`def f(x: Int) = x * x`
### Lambdas
```scala
(x: Int) => x*x
```
### Objetos
```scala
val a = (x: Int) => x*x // Objeto
a.apply(2)
val c = f _ // Convierte funcion a objeto
```
### Función de funciones
```scala
def g(h: Int=>Int) = h(3)
g(f)
def k(h: Int => Int)(x: Int) =h(x)
k(f)(4)
k f 4 // Version de scala vieja
```
### Métodos
```scala
object Util{
  def metodo(x: Int) = x+x
  val a = metodo _
}
```

#### Reto
Crear una función que genere una función para calcular si un número es mayor que el parametro que se le paso

##### Solución
```scala
// Con una función simple
def a(x: Int)(y: Int) = x > y
println(a(1)(2))
// Una función que resuelve el problema acorde al enunciado
def a(y: Int) = 6 > y
println(a(2))
// Agregando la parte de una función que genera una función
def a(x: Int) = { def y(x: Int) = 6 < x; y(x); }
println(a(2))
```

## Colecciones

Todas por defecto son inmutables

[Colecciones - Documentación](https://docs.scala-lang.org/overviews/collections-2.13/overview.html)
[Scala Docs](https://www.scala-lang.org/api/current/)

### Secuencias, conjuntos y mapas

#### Secuencias
```scala
a1 =Seq(1,2,3)
a1.appended(4)
val a2 = a1.appended(4)

// Alias
 a1 :+ 4
 a1.:+(4)
 a1 appended 4
```

#### Conjuntos
```scala
val c1 = Set(1,2,3)
val c2 = c1.incl(4)
// Alias
c1 + 4
c2(4) // Valida si existe
```

#### Mapas
```scala
val m1 = Map((1,"Hola"))
val m1 = Map(1 ->"Hola")
// Alias
val m2 = m1 + ((2, "hola"))
val m2 = m1 + (2 -> "hola")
```

#### Recorrer colecciones
```scala
// Para secuencias y conjuntos
a2.map(x => x+1)
// Para mapas
m2.view.mapValues(s => s + "!")
```

#### Reto
Crear un grupo de funciones para hallar la media, mediana y la moda de una lista de números, devolviendo la respuesta dentro de un tipo Map

```scala
object stats {
  // Para la media o promedio
  def media(l: Seq[Int]) : Double = l.sum.toDouble / l.size.toDouble
  // Para la mediana
  def mediana(l: Seq[Int]) : Double = {
    if(l.size % 2 == 1) l(l.size/2) else (l(l.size / 2) + l((l.size / 2) - 1))/ 2
  }
  // Para la moda
  def moda(l: Seq[Int]) : Double = l.groupBy(group => group).maxBy( num => num._2.size)._1
  // Resultado
  def resultado(l: Seq[Int]) : Map[String, Double] = {
    Map(("Media", media(l)),("Medina", mediana(l)),("Moda",moda(l)))
  }
}
// Resultados
val lista = Seq(1, 6, 6, 5, 2, 3).sorted
stats.resultado(lista)
```

## Tuplas y Objetos

Mientras que en OOP las clases tienen atributos y métodos en FP pueden tener únicamente atributos o únicamente métodos

```scala
val tupla = (1, "Daniel", false)
case class Persona(id: Int, nombre: String, activo: Boolean)

// Instanciar objeto
val p = Persona(1, "Daniel", true)

// Acceso a datos
tupla._3
p.nombre

// Objetos y tuplas
Persona.tupled(tupla)
Persona.unapply(p)

// Dos case class
case class A(id: Int)
case class B(id: Int, a: A)

// obj1 es inmutable
val obj1 = B(1, A(0))
// obj2 es la copia con una copia interna modificada.
val obj2 = obj1.copy(a = obj1.a.copy(1))
```

obj1.a.copy no necesita el nombre del atributo puesto que la clase A solo tiene uno. En cambio el obj1.copy sí necesita el nombre del atributo debido a que la clase B tiene varios. Es caótico tener muchos objetos internos, se debe de evitar

Algunas librerias que usan este concepto

### Lens
Un Lens es una función que hace más simple mirar y modificar valores internos dentro de una estructura de datos sin necesidad de demasiado código.

* Quicklens: Una de las más sencillas para usar y entender el concepto. https://github.com/softwaremill/quicklens
* Monocle: Más potente que la anterior, y basada en otra librería de Lens que se usa en Haskell. https://github.com/julien-truffaut/Monocle

### Pattern Machine

```scala
"hola" match {
  case "mundo" => "oo"
  case "hola" => "aa"
}

def g(x: Seq[Int]) = x match {
  case List(a, b, c) => a + b +c
  case List(a, b, c, d) => a + b + c + d
  case _ => 0
}

case class Persona(nombre: String, edad: Int)
val p1 = Persona("Maria", 20)
val p2 = Persona("Fredy", 15)

def h(x: Persona) = x match {
  case y if y.edad >= 18 => "Mayor de edad"
  case _ => "No es mayor de edad"
}
```

#### Reto
Mejorar el último match, para responder diferente, si alguien se llama María
y es mayor de edad, cuando se llama Maria y es menor de edad y los demás casos posibles

```scala
object Eval {
  case class Persona(nombre: String, edad: Int)
  def edad(x: Int) : String = if (x > 18) "Mayor de edad" else "No es mayor de edad"
  def h(x: Persona) : String = x match {
      case Persona ("Maria", _) => "Es Maria y " + edad (x.edad)
      case Persona (_, _) => "No es Maria y " + edad (x.edad)
    }
}
val p1 = Eval.Persona("Maria", 20)
val p2 = Eval.Persona("Fredy", 15)
Eval.h(p1)
Eval.h(p2)
```

## Tail Recursion

```scala
// Recursividad ineficiente
def factorial(n: Long) : Long = 
  if(n == 0) 1 
  else n * factorial(n - 1)
println(factorial(4))

// Recursividad en colada
// Calculo en cada iteración
def factorial(n: Long, res: Long = 1L) : Long = 
  if(n == 0)
    res
  else
    factorial(n - 1, n * res)

println(factorial(4))
```

## Agregaciones
Al trabajar con listas se necesita hacer acumulaciones para agregar datos a un resultado final

### FoldLeft y FoldRight
```scala
var result = (1 to 3).foldLeft(1L)((r,n) => n*r)
println(result)
```

## Conceptos teoricos de la programación funcional

### Funciones
* Función total. Puede dar respuesta a todos los elementos de su dominio. Son más seguras de utilizar

* Función parcial. No es capas de responder satisfactoriamente a todos los casos posibles

#### Ejemplos
```scala
val a = List(1,2,3,4)
a.head()
a.tail
val b = Nil
b.take(1)
b.drop(1)
a.take(1)
a.drop(1)
```

### Razonamiento inductivo
Forma de razonamiento donde el primer paso es encontrar el caso base, un hecho concreto que podemos usar como inicio del proceso.

El siguiente paso es modelar la continuación (paso inductivo). El resultado se convierte en el nuevo caso base.

En matemáticas es usado en las preposiciones. Una preposición es algo que afirmas, pero que puede ser falso o verdadero.

### Razonamiento con tipos
#### Tipados vs Dinámicos
Los tipos de dato son importantes para crear sistemas que escalen
Los tipos mantienen la consistencia de un sistema, y se vuelven una manera de probarlo

#### Sistema de tipos
Parte critica para haceer abstracciones. Son una forma de documentar las posibilidades que tenemos

Scala es más flexible y potente que Java

#### Lenguage ML
(Meta Lenguaje) Ha influenciado muchos lenguajes funcionales

#### Proposiciones y pruebas
Los tipos son proposiciones
Los programas son pruebas

El compilador se convierte en un razonador que prueba nuestro software con base a las proposiciones que le damos

#### Tipos vs Strings
Si se usan Strings el compilador no tiene forma de validar los datos, en lenguajes funcionales eso no sucede

#### Razonamiento de tipos
* Class
* Trail
* Type Alias `type PersonaId = Int`
* Case class `case class Persona(id: PersonaId, nombre:String)`
* Generics

```scala
type Estudiante = Persona
new Estudiante(1, "Daniel")
```

#### Traits

Existe un concepto de programación que se conoce como Mixin. Un mixin es algo que contiene funcionalidades que luego pueden ser juntadas en una sola entidad.

Los traits son la forma de aplicar este concepto. Dentro de un Trait podremos colocar cualquier implementación de funciones y/o valores, puede ser mezclada en un sólo objeto (como con la herencia)

```scala
trait ejemplo {
  val valor = 5
  def funcion(x: Int) = x * x
}
// Para instanciarse debe usarse un objeto
object ejemplo extends ejemplo
```

Son similares a las clases y se suelen usar para agrupar código, en lugar de tener funciones sueltas

#### Tipos genericos
Se comporta como un comodín. Nos permite tener certeza sobre un tipo sin saber cuál es aún

Se deben de usar con cuidado ya que pueden ser peligrosos. Pueden definirse en funciones, traits y clases

```scala
def f[A](x:A): String = s"$x"
f(23)
f(true)

trait ejemplo[A,B]{
  def g(x: A, f:A => B): B = f(x)
}
object ejemplo extends ejemplo[Int, String]
ejemplo.g(3,f)
```

#### Tipos de datos algebraicos

* Tipos suma (Implementar con Sealed traits)
  * Boolean
  * Error
  * etc...

* Tipos producto (Implementar con case, class, o traits)
  * String
  * Geometry
  * Persona
  * etc.

Saber esto nos guiara a saber la mejor manera de implementarlo

```scala
sealed trait Persona
case class Estudiante(nombre:String) extends Persona
case class Profesor(nombre:String, profesion: String) extends Persona
val me: Persona = Profesor("Daniel", "Desarrollador")

// Sealed valida que se cumplan los casos para ambos en persona
// En este caso avisa que puede faltar el estudiante
me match{
  case Profesor(nombre, profesion) => s"$nombre, es $profesion"
}
// Obliga a corregir a lo siguiente
me match{
  case Profesor(nombre, profesion) => s"$nombre, es $profesion"
  case Estudiante(nombre) => s"$nombre es estudiante"
}
```

#### Evaluación peresoza o Lazy
Evita la evaluación de un valor hasta que sea utilizado
Las colecciones pueden ser peresozas con LazyList

```scala
// y Se instancia hasta que se usa
lazy val y = x -1
lazy val x = 100
println(y)
```

Los elementos de la lista se vuelven peresozos
```scala
val a = LazyList(1,2,3)
```

Entrenamiento de maquinas o valores grandes, optimizaciones, rendimiento e inyección de dependencias

#### Disyunciones
Es algo que puede explicar dos cosas a la vez

##### Option
Intenta solucionar el problema de los nulos
Las referencias nulas han sido historicamente un problema

Los valores nulos se pueden tratar como inexistentes y tratarse sólo cuando existan

```scala
val a = Option(2)
val b = Option(null)

// Option en listas
val lista = List(1,2,3)
lista.find(x => x == 3)
lista.find(x => x == 4)

lista.find(x => x == 3).map(_+1)
lista.find(x => x == 4).map(_+1) 

// Option de option
lista.find(x => x == 3).map(x=>lista.headOption.map(y=>x+y))
// flatMap ayuda a obtener sólo un option
val resultado = lista.find(x => x == 3).flatMap(x=>lista.headOption.map(y=>x+y))
// Forma insegura de obtener datos
resultado.get
// Forma segura
resultado.getOrElse(0)
```

##### Try
Trata las excepciones como valores y no saltar abruptamente en la ejecución del código

No está por defecto, hay que importarlo
```scala
import scala.util.Try
val a = Try(Nil.head)
a.map(x => s"$x")
Right(10).map(_+1)
Left[Int,Int](10).map(_+1)
```


##### Either
Aplicable a cualquier contexto, camino feliz y camino triste

# Programación con SPARK

## RDD
Un RDD, según Spark, se define como una colección de elementos que es tolerante a fallos y que es capaz de operar en paralelo.

* Es la principal abstracción de datos, el tipo de dato básico que tiene Apache Spark.
* Los RDD están particionados en los distintos nodos del clúster, ya que Apache Spark se suele instalar en un clúster o conjunto de máquinas, por lo que esos RRDs se encuentran distribuidos sobre esas máquinas . Con ello se consigue la tolerancia a fallos, porque si falla una máquina tenemos el fichero en otras máquinas.
* Se suelen crear a partir de un fichero del HDFS , el sistema de ficheros distribuidos de Hadoop.
*  Usan la evaluación perezosa, que consiste en que todas las transformaciones que vayamos haciendo a los RDDs se van a ir almacenando en un DAG y no se van a resolver hasta que no quede más remedio , hasta que la herramienta esté obligada a realizarlas. Esta evaluación perezosa tiene una ventaja y un inconveniente, la primera es que ganamos tiempo, y el inconveniente es que si falla, no lo vamos a ver hasta que se resuelva el grafo completo.

### SparkConf y SparkContext

Son el tipo de objeto básico a la hora de comenzar a crear los RDDs.

 * <b>SparkContext</b> nos va a especificar cómo vamos a acceder a nuestro clúster.
 * <b>SparkConf</b> va a tener información acerca de nuestra aplicación, ya sea el número de hilos, la memoria que queramos que consuma, etc.