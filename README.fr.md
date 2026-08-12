# ThreadWeave

🇬🇧 [English](README.md) | 🇫🇷 Français

> **Un moteur d'exécution distribué orienté ressources pour les traitements modernes.**
>
> Construisez des systèmes distribués sans lier votre infrastructure à un langage de programmation.

ThreadWeave est une plateforme open source d'exécution distribuée construite autour d'une idée simple :

**Le moteur d'exécution ne devrait pas dépendre du langage dans lequel votre application est écrite.**

Plutôt que d'être une nouvelle file de tâches Python, ThreadWeave fournit un moteur générique chargé de l'orchestration : planification, gestion des ressources, supervision, reprise après incident, observabilité et exécution distribuée.

Le code métier reste dans votre langage.

L'infrastructure reste en Rust.

---

## Pourquoi ThreadWeave ?

Les systèmes distribués modernes ont besoin de bien plus qu'une simple file de tâches.

Ils nécessitent notamment :

* une planification orientée ressources (CPU, mémoire, GPU…)
* une reprise après incident
* la gestion des traitements longs
* une observabilité native
* une montée en charge horizontale
* un support multi-langage

ThreadWeave apporte ces capacités grâce à un protocole d'exécution générique et des runtimes spécialisés.

---

## Principes fondateurs

ThreadWeave repose sur quelques idées fortes :

* 🌍 **Language Agnostic First** — le cœur ne dépend d'aucun langage.
* ⚙️ **Le moteur orchestre, les runtimes exécutent**.
* 🧠 **Les ressources sont des citoyens de première classe**.
* 📡 **Tout est observable**.
* 🔌 **Architecture orientée plugins**.
* 🦀 **Rust au cœur du moteur** pour la fiabilité et les performances.

---

## Architecture

```text
             +----------------------+
             |   Moteur Rust Core   |
             +----------------------+
                      |
      +---------------+---------------+
      |               |               |
 Runtime Python  Runtime JS     Runtime Java
      |               |               |
 Code métier     Code métier     Code métier
```

Le moteur Rust est responsable de :

* la planification
* l'allocation des ressources
* la coordination distribuée
* les retries
* les timeouts
* la supervision
* la reprise après incident

Les runtimes exécutent uniquement le code utilisateur.

---

## État du projet

ThreadWeave est actuellement en cours de développement.

### POC de soumission Redis

Le moteur Rust implémente actuellement uniquement la première étape de soumission :

1. `SubmitTask` reçoit une commande protobuf via gRPC.
2. Le moteur l'encapsule dans un `BrokerEnvelope` versionné.
3. L'enveloppe est ajoutée à la liste Redis `threadweave:broker:tasks`.
4. Le Job est renvoyé avec l'état `ACCEPTED` seulement après confirmation de Redis.

Pour construire l'image optimisée et démarrer toute la stack :

```bash
export BUF_TOKEN="<votre-jeton-buf>"
docker compose up --build -d
```

Le jeton est uniquement transmis comme secret BuildKit pour télécharger depuis
Buf les crates Rust générés du protocole ; il n'est pas conservé dans l'image.
Pour les commandes Cargo locales, authentifiez-vous une fois avec
`cargo login --registry buf "Bearer <votre-jeton-buf>"`.

Le service gRPC est alors disponible sur `localhost:50051`. Le port peut être
modifié, sans reconstruire l'image, avec par exemple
`THREADWEAVE_PORT=6000 docker compose up --build -d`. Utilisez
`docker compose logs -f threadweave` pour suivre le moteur et
`docker compose down` pour arrêter la stack (ajoutez `-v` pour supprimer aussi
les données Redis).

L'image est construite en plusieurs étapes : la toolchain Rust reste dans
l'étape de compilation et l'image finale, statique, ne contient que le binaire
optimisé et sa configuration. Elle s'exécute sans privilèges et avec un système
de fichiers en lecture seule.

Pour le développement local sans conteneur, démarrez uniquement Redis puis le
moteur :

```bash
docker compose up -d redis
cargo run
```

Par défaut, le moteur charge `threadweave.yaml`. Un autre fichier de
configuration peut être sélectionné depuis la CLI :

```bash
cargo run -- --config /chemin/vers/threadweave.yaml
```

Le fichier YAML configure l'adresse d'écoute gRPC, l'URL Redis, le préfixe des
clés du broker et la destination des tâches. Les valeurs par défaut sont dans
`threadweave.yaml`.

Le broker est abstrait par le trait `Broker`, tandis que le stockage des
résultats possède un trait séparé `BackendResult`. L'ordonnancement, la
consommation et la persistance des résultats ne sont pas encore implémentés.

La priorité est donnée à la conception de l'architecture et de la documentation avant l'implémentation des fonctionnalités.

Les travaux en cours portent sur :

* la documentation
* les RFCs
* l'architecture du moteur
* le cœur Rust
* le runtime Python

---

## Feuille de route

* ✅ Vision du projet
* ✅ Processus RFC
* 🚧 Cœur Rust
* 🚧 Site de documentation
* ⏳ Runtime Python
* ⏳ Scheduler distribué
* ⏳ Gestionnaire de ressources
* ⏳ Runtime JavaScript
* ⏳ Runtime Java
* ⏳ Tableau de bord Web

---

## Open Source

ThreadWeave est développé de manière ouverte.

Toutes les contributions sont les bienvenues : RFCs, discussions, rapports de bugs, nouveaux runtimes, schedulers, backends de stockage ou outils.

L'architecture est pensée pour être extensible dès sa conception.

---

## Documentation

La documentation est rédigée avant l'implémentation.

Ce dépôt contient le code source.

La documentation complète, les RFCs et les documents d'architecture sont disponibles sur le site de documentation.

---

## Publier une version

Ajoutez un jeton d'API crates.io aux secrets GitHub Actions du dépôt sous le nom
`CRATES_IO_TOKEN`. GitHub Actions publie la crate sur crates.io ainsi que des
binaires précompilés pour Linux, macOS et Windows lorsqu'un tag de version
sémantique est poussé. Le tag doit correspondre à la version définie dans
`Cargo.toml`.

```bash
git tag v0.1.0
git push origin v0.1.0
```

Le workflow publie d'abord la crate, puis crée la GitHub Release correspondante
et génère automatiquement ses notes de version.

---

## Licence

Distribué sous licence Apache 2.0.

---

## Vision

Nous pensons que le choix d'un langage ne devrait jamais imposer le choix d'une infrastructure distribuée.

Notre ambition est simple :

**Un moteur d'exécution. Tous les langages. Tous les workloads.**
