"""GHArchiveSensor — ВАШ custom sensor. Специфікація: ../../SPEC.md → «Sensor».

Сенсор чекає, поки годинний файл GitHub Archive за logical date стане доступним,
і лише тоді пропускає DAG далі.

Підказки:
  * успадкуйте `airflow.sensors.base.BaseSensorOperator`;
  * у __init__ прийміть параметр `hour` (година доби, яку перевіряємо);
  * реалізуйте `poke(self, context) -> bool`: візьміть дату з context["ds"],
    зберіть URL https://data.gharchive.org/<ds>-<hour>.json.gz і зробіть HTTP HEAD —
    поверніть True на 200, інакше False (або при винятку);
  * у DAG додайте сенсор першою задачею з timeout=600, poke_interval=60,
    mode="reschedule".
"""

from __future__ import annotations

import urllib.error
import urllib.request

from airflow.sensors.base import BaseSensorOperator


class GHArchiveSensor(BaseSensorOperator):
    def __init__(self, hour: int = 14, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hour = hour

    def poke(self, context) -> bool:
        # TODO: HEAD-запит до gharchive за context["ds"] і self.hour; True, якщо 200.
        ds = context["ds"]
        url = f"https://data.gharchive.org/{ds}-{self.hour}.json.gz"
        request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "gh-airflow-homework/1.0"})
        try:
            with urllib.request.urlopen(request) as response:
                return response.getcode() == 200
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):

            return False
        except Exception:
            raise NotImplementedError("Реалізуйте GHArchiveSensor.poke — див. SPEC.md")
