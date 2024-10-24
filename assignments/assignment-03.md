# Assignment 03

Full name (Student ID Number)

You can answer in English or Bahasa Indonesia.

## Problem 1
Explore the following documentation about map and struct creation in PySpark.
You can try run two examples from 
[StructType](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html)
and [map_from_entries](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.map_from_entries.html)

With the following input `.json` file

**mobile-legend.json**
```json
{
  "name": "Mobile Legends: Bang Bang",
  "url": "https://mobile-legends.fandom.com/wiki/", 
  "section": { 
    "EXPLORE": null,
    "MLBB": null,
    "GAME MODES": null,
    "SHOP": null,
    "HEROES": {
      "Hero Roles": {},
      "Hero Specialties I": {},
      "Hero Specialties II": {},
      "Laning Recommendation": {},
      "List of heroes": {
        "List": [
          {
            "icon": "https://mobile-legends.fandom.com/wiki/Miya",
            "hero_name": "Miya",
            "alias": "the Moonligh Archer",
            "hero_order": 1,
            "role": ["Marksman"],
            "specialty":  ["Finisher", "Damager"],
            "lane": ["Gold Lane"],
            "region": "Azrya Woodlands",
            "price": { "battle_point": 10800, "ticket": 399, "diamond": null },
            "release_year": 2016
          },
          {
            "icon": "https://mobile-legends.fandom.com/wiki/Balmond",
            "hero_name": "Balmond",
            "alias": "the Bloody Beast",
            "hero_order": 2,
            "role": ["Fighter"],
            "specialty":  ["Damager", "Regen"],
            "lane": ["Jungling"],
            "region": "The Barren Lands",
            "price": { "battle_point": 6500, "ticket": null, "diamond": 299 },
            "release_year": 2016
          },
          {
            "icon": "https://mobile-legends.fandom.com/wiki/Saber",
            "hero_name": "Saber",
            "alias": "the Wandering Sword",
            "hero_order": 3,
            "role": ["Assassin"],
            "specialty":  ["Charge", "Finisher"],
            "lane": ["Jungling", "Roaming"],
            "region": "Laboratory 1718",
            "price": { "battle_point": 6500, "ticket": null, "diamond": 299 },
            "release_year": 2016
          },
          {
            "icon": "https://mobile-legends.fandom.com/wiki/Alice",
            "hero_name": "Alice",
            "alias": "the Queen Blood",
            "hero_order": 4,
            "role": ["Mage", "Tank"],
            "specialty":  ["Charge", "Regen"],
            "lane": ["EXP Lane", "Jungling"],
            "region": "The Barren Lands",
            "price": { "battle_point": 15000, "ticket": null, "diamond": 399 },
            "release_year": 2016
          },
          {
            "icon": "https://mobile-legends.fandom.com/wiki/Nana",
            "hero_name": "Nana",
            "alias": "the Sweet Leonin",
            "hero_order": 5,
            "role": ["Mage"],
            "specialty":  ["Poke", "Burst"],
            "lane": ["Mid Lane"],
            "region": "Azrya Woodlands",
            "price": { "battle_point": 6500, "ticket": null, "diamond": 299 },
            "release_year": 2016
          },
        ],
        "History": {
          "Role": [
            {
              "name": "Patch Notes",
              "patch_num": "1.8.2",
              "year": 2023,
              "patch": [
                { "hero_name": "Akai", "patch_stat": "REMOVED ROLE -", "patch_detail": "Tank / Support -> Tank", },
                { "hero_name": "Atlas", "patch_stat": "REMOVED ROLE -", "patch_detail": "Tank / Support -> Tank", },
                { "hero_name": "Franco", "patch_stat": "REMOVED ROLE -", "patch_detail": "Tank / Support -> Tank", },
                { "hero_name": "Khufra", "patch_stat": "REMOVED ROLE -", "patch_detail": "Tank / Support -> Tank", },
                { "hero_name": "Tigreal", "patch_stat": "REMOVED ROLE -", "patch_detail": "Tank / Support -> Tank", },
                { "hero_name": "Nana", "patch_stat": "REMOVED ROLE -", "patch_detail": "Mage / Support -> Mage", }
              ]
            },
          ],
          "Specialty": {
            "name": "Patch Notes",
            "patch_num": "1.6.10",
            "year": null,
            "patch": [
              { "hero_name": "Kimmy", "patch_stat": "PARTIALLY ADJUSTED -", "patch_detailed": "Damage / Mixed Damage -> Damage / Magic Damage", },
              { "hero_name": "Natan", "patch_star": "ADJUSTED -", "patch_detailed": "Burst / Reap to Burst / Magic Damage" }
            ]
          }
        }
      },
      "Remodeled and Revamped Heroes": null ,
    }
  }
}
```

Create the following Dataframe

1. ```
   +-------------------------------------+-----------------------------------------------------------+
   | hero_name                           | role                                                      |
   +-------------------------------------+-----------------------------------------------------------+
   | [Miya, Balmond, Saber, Alice, Nana] | [[Marksman], [Fighter], [Assassin], [Mage, Tank], [Mage]] |
   +-------------------------------------+-----------------------------------------------------------+
   ```

2. ```
   +-----------------------+
   | hero_name -> role     |
   +-----------------------+
   | Miya -> [Marksman]    |
   | Balmond -> [Fighter]  | 
   | Saber -> [Assassin]   | 
   | Alice -> [Mage, Tank] | 
   | Nana -> [Mage]        |
   +-----------------------+
   ```


## Problem 2
Given the following tables, 

**bands**
```
+----+-------------------+
| id | name              |
+----+-------------------+
| 1  | Iron Maiden       |
| 2  | Deuce             |
| 3  | Avenged Sevenfold |
| 4  | Ankor             |
+----+-------------------|
```

**albums** 
```
+----+-------------------------+--------------+---------|
| id | name                    | release_year | band_id |
| 1  | The Number of the Beast |         1982 | 1       |
| 2  | Power Slave             |         1984 | 1       |
| 3  | Nightmare               |         2018 | 2       |
| 4  | Nightmare               |         2010 | 3       |
+----+-------------------------+--------------+---------+
```

perform 
- inner join (default)
- left and right outer join
- full outer join
- left semi-join and left anti-join
- cross join

to the common column `id` in **bands** and column `band_id`
in **albums**.

Hint: First you need to write those two tables into `.csv`  files.
and perform joining operations.