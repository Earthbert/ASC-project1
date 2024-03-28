Corecții Readme

# Tema 1

Nume: Daraban Albert Timotei

Grupă: 334CA

## Organizare

### ThreadPool

Fișier: task_runner.py

Pentru ThreadPool am ales să folosesc un ThreadPoolExecutor și un dicționar care face legătura între job_id și future-ul acestuia. Pentru a nu se supraîncărca acest dicționar, am un thread care, la un interval de timp, scoate future-urile terminate din dicționar. De asemenea, acestea se pot elimina și atunci când un utilizator cere rezultatul unui job_id și este terminat sau când un utilizator cere starea job-urilor. Pentru accesul la dicționar folosesc un lock.

### Rute

Fișier: routes.py

Pentru requesturi, dau submit pe ThreadPool cu task-uri din fișierul threadpool_tasks.py.

Pentru cereri de răspuns, verific dacă e terminat și returnez datele calculate.

### Data Ingestor

Fișier: data_ingestor.py

Conține o listă de dicționare, care au ca cheie numele coloanei din care face parte valoarea, și o funcție care returnează doar elementele listei care răspund la o întrebare dată.

### Task-uri

Fișier: threadpool_tasks.py

Conține logica de selectare a datelor pentru toate tipurile de request-uri.

### Întrebări

#### Consideri că tema este utilă?

Da, consider că m-a ajutat să aprofundez limbajul Python.

#### Consideri implementarea naivă, eficientă, se putea mai bine?

Implementarea este una simplă, făcută să meargă.

Ar fi fost mai eficientă dacă ar fi existat caching de răspunsuri și dacă nu ar fi folosit un lock peste dicționarul de futures.

Git: <https://github.com/Earthbert/ASC-assignment1>
