# Tema 1

Nume: Daraban Albert Timotei

Grupă: 334CA

## Organizare

### ThreadPool

Fisierul: ``task_runner.py``

Pentru ThreadPool am ales sa folosit un ThreadPoolExecutor si un dictionar care face legatura intre job_id-ul si future-ul acestuia. Pentru a nu se supra incarca acest dictionar am un thread care o data la un interval de timp scoate future urile terminate din dictionar, de asemenea acestea se pot elimina si atunci cand un user cere rezultatul unui job_id si este terminat si atunci cand un user cere starea job-urilor. Pentru accesul la dictionar folosesc un lock.

### Routes

Fisierul: ``routes.py``

Pentru requesturi dau submit pe ThreadPool cu task-uri din fisierul ``threadpool_tasks.py``.
Pentru cereri de raspuns verific daca e terminat si returnez datele calculate.

### Data Ingestor

Fisierul: ``data_ingestor.py``

Contine o lista de dictionare, care au ca cheie numele coloaneai din care face parte valoare, si o functie care returneza doar elementele listei care raspund la o intrebare data.

### Tasks

Fisierul: ``threadpool_tasks.py``

Contine logica de selectare a datelor pentru toate tipurile de request-uri.

### Intrebari

* Consideri că tema este utilă?

  Da, consider ca m a ajutat sa aprofundez limbaj ul python.

* Consideri implementarea naivă, eficientă, se putea mai bine?

  Implementarea este una simpla, facuta sa mearga.
  Ar fi fost mai eficienta daca ar fi exista caching de raspunsuri. Si daca nu ar folosi un lock peste dictionarul de futures.

Git: <https://github.com/Earthbert/ASC-assignment1>
