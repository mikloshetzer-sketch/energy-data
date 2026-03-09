<h2>Regionális hatások</h2>

<style>
.regional-impact-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  gap: 16px;
  margin-top: 20px;
  margin-bottom: 30px;
}

.regional-impact-card {
  background: #f5f7fa;
  border: 1px solid #d9e2ec;
  border-radius: 10px;
  padding: 18px;
  box-shadow: 0 2px 6px rgba(0,0,0,0.06);
}

.regional-impact-title {
  font-size: 20px;
  font-weight: 700;
  color: #111827 !important;
  margin-bottom: 8px;
}

.regional-impact-label {
  font-size: 16px;
  font-weight: 700;
  margin-bottom: 10px;
}

.regional-impact-label.low {
  color: #15803d !important;
}

.regional-impact-label.moderate {
  color: #92400e !important;
}

.regional-impact-label.high {
  color: #c2410c !important;
}

.regional-impact-label.extreme {
  color: #b91c1c !important;
}

.regional-impact-text {
  font-size: 14px;
  line-height: 1.7;
  color: #111827 !important;
}

@media (max-width: 700px) {
  .regional-impact-grid {
    grid-template-columns: 1fr;
  }
}
</style>

<div class="regional-impact-grid">

  <div class="regional-impact-card">
    <div class="regional-impact-title">🇪🇺 Európa</div>
    <div class="regional-impact-label" id="regional-europe-label">betöltés...</div>
    <div class="regional-impact-text" id="regional-europe-text">betöltés...</div>
  </div>

  <div class="regional-impact-card">
    <div class="regional-impact-title">🇺🇸 Amerika</div>
    <div class="regional-impact-label" id="regional-america-label">betöltés...</div>
    <div class="regional-impact-text" id="regional-america-text">betöltés...</div>
  </div>

  <div class="regional-impact-card">
    <div class="regional-impact-title">🌏 Ázsia</div>
    <div class="regional-impact-label" id="regional-asia-label">betöltés...</div>
    <div class="regional-impact-text" id="regional-asia-text">betöltés...</div>
  </div>

</div>

<script>
document.addEventListener("DOMContentLoaded", function () {

  function fillRegion(prefix, data) {
    const labelEl = document.getElementById(prefix + "-label");
    const textEl = document.getElementById(prefix + "-text");

    const label = data && data.label ? data.label : "nincs adat";
    const text = data && data.text ? data.text : "nincs adat";
    const level = data && data.level ? data.level : "";

    labelEl.innerText = label;
    textEl.innerText = text;

    labelEl.classList.remove("low", "moderate", "high", "extreme");
    if (level) {
      labelEl.classList.add(level);
    }
  }

  fetch("https://raw.githubusercontent.com/mikloshetzer-sketch/energy-data/main/oil-data.json")
    .then(function(response){
      if(!response.ok){throw new Error("Hálózati hiba")}
      return response.json()
    })
    .then(function(data){

      const regional = data.regional_impact || {};

      fillRegion("regional-europe", regional.europe);
      fillRegion("regional-america", regional.america);
      fillRegion("regional-asia", regional.asia);

    })
    .catch(function(){
      document.getElementById("regional-europe-label").innerText = "adat nem elérhető";
      document.getElementById("regional-europe-text").innerText = "A régiós adat nem tölthető be.";
      document.getElementById("regional-america-label").innerText = "adat nem elérhető";
      document.getElementById("regional-america-text").innerText = "A régiós adat nem tölthető be.";
      document.getElementById("regional-asia-label").innerText = "adat nem elérhető";
      document.getElementById("regional-asia-text").innerText = "A régiós adat nem tölthető be.";
    });

});
</script>
