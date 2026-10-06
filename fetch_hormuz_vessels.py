#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hormuz Vessel Monitor v1.3 - persistent AIS + micro-geofences."""
from __future__ import annotations
import json, math, sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

VERSION="1.3-micro-geofence-persistent-tracking"
BASE="https://hormuz.data-tracking.net"; ENDPOINT="/api/ships"; OUT=Path("hormuz-vessels.json")
NEW_RUNS=2; MISSING_RUNS=2; MAX_MISSING=6; MAX_HIST=30
OIL={"crude oil tanker","tanker","vlcc/ulcc","oil tanker","oil/chemical tanker","oil products tanker","product tanker","product/chemical tanker","product/chem tanker","chemical/oil products tanker","asphalt/bitumen tanker"}
CRUDE={"crude oil tanker","vlcc/ulcc"}
PRODUCT=OIL-CRUDE-{"tanker","oil tanker"}
MICRO_PATTERNS=[["hormuz_west","hormuz_core","hormuz_east"],["hormuz_east","hormuz_core","hormuz_west"]]
BROAD_PATTERNS=[["persian_gulf","strait","gulf_of_oman"],["gulf_of_oman","strait","persian_gulf"]]
LAT0,LAT1,LON0,LON1,WEST,CORE=25.75,27.35,55.65,57.25,56.20,56.65

def now(): return datetime.now(timezone.utc).isoformat()
def text(x): return "" if x is None else str(x).strip()
def num(x):
    try:return None if x is None else float(x)
    except:return None
def get(d,*keys):
    for k in keys:
        if d.get(k) is not None:return d[k]
def zone(x):
    s=" ".join(text(x).lower().replace("_"," ").replace("-"," ").split())
    return {"persian gulf":"persian_gulf","gulf":"persian_gulf","gulf of oman":"gulf_of_oman","oman gulf":"gulf_of_oman","strait":"strait","hormuz":"strait","strait of hormuz":"strait"}.get(s)
def micro(lat,lon):
    if lat is None or lon is None or not(LAT0<=lat<=LAT1 and LON0<=lon<=LON1):return None
    return "hormuz_west" if lon<WEST else ("hormuz_core" if lon<=CORE else "hormuz_east")
def tanker_size(d,oil):
    if not oil:return "NOT_APPLICABLE"
    if d is None:return "UNKNOWN"
    if d>=320000:return "ULCC"
    if d>=200000:return "VLCC"
    if d>=120000:return "SUEZMAX"
    if d>=80000:return "AFRAMAX"
    if d>=55000:return "PANAMAX_LR1"
    if d>=25000:return "HANDYMAX_MR"
    return "SMALL"
def distance(a,b,c,d):
    if None in(a,b,c,d):return None
    r=6371.0088;p1=math.radians(a);p2=math.radians(c);dp=math.radians(c-a);dl=math.radians(d-b)
    q=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return round(2*r*math.asin(math.sqrt(q)),2)
def fetch():
    req=Request(BASE+ENDPOINT,headers={"User-Agent":"energy-data-hormuz/1.3","Accept":"application/json"})
    with urlopen(req,timeout=45) as r: raw=json.loads(r.read().decode())
    if isinstance(raw,list):return raw
    for k in("ships","vessels","data","results"):
        if isinstance(raw,dict) and isinstance(raw.get(k),list):return raw[k]
    raise RuntimeError("Vessel list not found")
def vessel_id(r):
    m=text(get(r,"mmsi","MMSI"));i=text(get(r,"imo","IMO"));n=text(get(r,"name","ship_name","vessel_name"))
    return "MMSI:"+m if m else ("IMO:"+i if i else ("NAME:"+n.upper() if n else "ANON:"+str(get(r,"latitude","lat"))+":"+str(get(r,"longitude","lon","lng"))))
def normalize(r):
    cat=text(get(r,"ship_category","category","ship_type","type")) or "Unknown";c=cat.lower()
    oil=c in OIL;crude=c in CRUDE;prod=c in PRODUCT;d=num(get(r,"dwt","deadweight","deadweight_tonnage"))
    lat=num(get(r,"latitude","lat"));lon=num(get(r,"longitude","lon","lng"));z=zone(get(r,"zone","region","area"));mz=micro(lat,lon)
    role="CRUDE" if crude else ("PRODUCT_OR_CHEMICAL" if prod else ("OIL_OTHER" if oil else "NON_OIL"))
    return {"vessel_id":vessel_id(r),"name":text(get(r,"name","ship_name","vessel_name")) or None,"imo":text(get(r,"imo","IMO")) or None,"mmsi":text(get(r,"mmsi","MMSI")) or None,"flag":text(get(r,"flag","country")) or None,"ship_category":cat,"dwt":d,"tanker_size":tanker_size(d,oil),"oil_related":oil,"crude_related":crude,"product_related":prod,"energy_role":role,"position":{"latitude":lat,"longitude":lon,"region":z,"micro_zone":mz},"navigation":{"direction":None,"speed":num(get(r,"speed","sog")),"course":num(get(r,"course","cog")),"heading":num(get(r,"heading","hdg")),"destination":text(get(r,"destination","dest")) or None},"last_observation":text(get(r,"timestamp","last_observation","last_seen","time")) or None,"raw":r}
def load_previous():
    try:return json.loads(OUT.read_text(encoding="utf-8"))
    except:return {}
def compact(h):
    out=[]
    for x in h:
        z=x.get("zone")
        if z and (not out or out[-1]!=z):out.append(z)
    return out
def event(v,a,b,t,d,layer):
    n=v["navigation"];return {"vessel_id":v["vessel_id"],"name":v["name"],"ship_category":v["ship_category"],"energy_role":v["energy_role"],"oil_related":v["oil_related"],"crude_related":v["crude_related"],"tanker_size":v["tanker_size"],"dwt":v["dwt"],"layer":layer,"from_region":a,"to_region":b,"distance_since_previous_km":d,"speed":n["speed"],"course":n["course"],"destination":n["destination"],"observed_at":t}
def migrate(p):
    s=p.get("persistent_vessel_state")
    if isinstance(s,dict):
        for r in s.values():
            r.setdefault("last_micro_zone",None);r.setdefault("micro_zone_history",[]);r.setdefault("confirmed_micro_transit_count",0);r.setdefault("last_micro_transit",None)
        return s
    return {}
def track(vs,p,t):
    state=migrate(p);cur={v["vessel_id"]:v for v in vs};old=set(state)
    names=["raw_appeared","raw_missing","confirmed_new","confirmed_disappeared","zone_changes","oil_tanker_zone_changes","large_tanker_zone_changes","vlcc_ulcc_zone_changes","micro_zone_changes","oil_tanker_micro_zone_changes","large_tanker_micro_zone_changes","vlcc_ulcc_micro_zone_changes","confirmed_hormuz_transits","possible_direct_transits","confirmed_micro_hormuz_transits","possible_micro_skipped_core_transits"]
    e={k:[] for k in names}
    for i,v in cur.items():
        p0=v["position"];z=p0["region"];mz=p0["micro_zone"]
        if i not in state:
            state[i]={"vessel_id":i,"name":v["name"],"ship_category":v["ship_category"],"energy_role":v["energy_role"],"oil_related":v["oil_related"],"crude_related":v["crude_related"],"product_related":v["product_related"],"tanker_size":v["tanker_size"],"dwt":v["dwt"],"first_seen":t,"last_seen":t,"seen_runs":1,"consecutive_seen_runs":1,"missing_runs":0,"confirmed_present":False,"confirmed_disappeared":False,"last_zone":z,"zone_history":[{"zone":z,"observed_at":t}] if z else [],"last_micro_zone":mz,"micro_zone_history":[{"zone":mz,"observed_at":t}] if mz else [],"confirmed_transit_count":0,"confirmed_micro_transit_count":0,"last_transit":None,"last_micro_transit":None,"last_position":{"latitude":p0["latitude"],"longitude":p0["longitude"]}}
            e["raw_appeared"].append(v);continue
        r=state[i];miss=int(r.get("missing_runs") or 0);oz=r.get("last_zone");om=r.get("last_micro_zone");op=r.get("last_position") or {};d=distance(op.get("latitude"),op.get("longitude"),p0["latitude"],p0["longitude"])
        for k in("name","ship_category","energy_role","oil_related","crude_related","product_related","tanker_size","dwt"):r[k]=v.get(k)
        r["last_seen"]=t;r["seen_runs"]=int(r.get("seen_runs") or 0)+1;r["consecutive_seen_runs"]=1 if miss else int(r.get("consecutive_seen_runs") or 0)+1;r["missing_runs"]=0;r["confirmed_disappeared"]=False
        if r["consecutive_seen_runs"]>=NEW_RUNS:
            was=bool(r.get("confirmed_present"));r["confirmed_present"]=True
            if not was:e["confirmed_new"].append(v)
        if miss==0 and oz and z and oz!=z:
            x=event(v,oz,z,t,d,"broad_zone");e["zone_changes"].append(x)
            if v["oil_related"]:
                e["oil_tanker_zone_changes"].append(x)
                if (v["dwt"] or 0)>=100000:e["large_tanker_zone_changes"].append(x)
                if v["tanker_size"] in("VLCC","ULCC"):e["vlcc_ulcc_zone_changes"].append(x)
            r.setdefault("zone_history",[]).append({"zone":z,"observed_at":t});r["zone_history"]=r["zone_history"][-20:];seq=compact(r["zone_history"])
            for pat in BROAD_PATTERNS:
                if len(seq)>=3 and seq[-3:]==pat:
                    y={**x,"direction":"OUTBOUND" if pat[0]=="persian_gulf" else "INBOUND","pattern":pat,"confirmation":"CONFIRMED_BROAD_THREE_ZONE"};e["confirmed_hormuz_transits"].append(y);r["confirmed_transit_count"]=int(r.get("confirmed_transit_count") or 0)+1;r["last_transit"]=y;break
            if {oz,z}=={"persian_gulf","gulf_of_oman"}:e["possible_direct_transits"].append({**x,"direction":"OUTBOUND" if oz=="persian_gulf" else "INBOUND","confirmation":"POSSIBLE_SKIPPED_STRAIT_ZONE"})
        if miss==0 and om and mz and om!=mz:
            x=event(v,om,mz,t,d,"micro_zone");e["micro_zone_changes"].append(x)
            if v["oil_related"]:
                e["oil_tanker_micro_zone_changes"].append(x)
                if (v["dwt"] or 0)>=100000:e["large_tanker_micro_zone_changes"].append(x)
                if v["tanker_size"] in("VLCC","ULCC"):e["vlcc_ulcc_micro_zone_changes"].append(x)
            r.setdefault("micro_zone_history",[]).append({"zone":mz,"observed_at":t});r["micro_zone_history"]=r["micro_zone_history"][-30:];seq=compact(r["micro_zone_history"])
            for pat in MICRO_PATTERNS:
                if len(seq)>=3 and seq[-3:]==pat:
                    y={**x,"direction":"OUTBOUND" if pat[0]=="hormuz_west" else "INBOUND","pattern":pat,"confirmation":"CONFIRMED_MICRO_WEST_CORE_EAST"};e["confirmed_micro_hormuz_transits"].append(y);r["confirmed_micro_transit_count"]=int(r.get("confirmed_micro_transit_count") or 0)+1;r["last_micro_transit"]=y;break
            if {om,mz}=={"hormuz_west","hormuz_east"}:e["possible_micro_skipped_core_transits"].append({**x,"direction":"OUTBOUND" if om=="hormuz_west" else "INBOUND","confirmation":"POSSIBLE_SKIPPED_MICRO_CORE"})
        r["last_zone"]=z;r["last_micro_zone"]=mz;r["last_position"]={"latitude":p0["latitude"],"longitude":p0["longitude"]}
    for i in old-set(cur):
        r=state[i];r["missing_runs"]=int(r.get("missing_runs") or 0)+1;r["consecutive_seen_runs"]=0
        x={"vessel_id":i,"name":r.get("name"),"ship_category":r.get("ship_category"),"energy_role":r.get("energy_role"),"tanker_size":r.get("tanker_size"),"dwt":r.get("dwt"),"last_zone":r.get("last_zone"),"last_micro_zone":r.get("last_micro_zone"),"last_seen":r.get("last_seen"),"missing_runs":r["missing_runs"]};e["raw_missing"].append(x)
        if r["missing_runs"]>=MISSING_RUNS:
            if not r.get("confirmed_disappeared"):e["confirmed_disappeared"].append(x)
            r["confirmed_disappeared"]=True;r["confirmed_present"]=False
    prune=[i for i,r in state.items() if int(r.get("missing_runs") or 0)>MAX_MISSING]
    for i in prune:state.pop(i,None)
    s={"current_observed_vessels":len(vs),"persistent_state_vessels":len(state),"confirmed_present_vessels":sum(bool(r.get("confirmed_present")) for r in state.values()),"temporarily_missing_vessels":sum(0<int(r.get("missing_runs") or 0)<MISSING_RUNS for r in state.values()),"confirmed_disappeared_vessels":sum(bool(r.get("confirmed_disappeared")) for r in state.values()),"raw_appeared_this_run":len(e["raw_appeared"]),"raw_missing_this_run":len(e["raw_missing"]),"confirmed_new_this_run":len(e["confirmed_new"]),"confirmed_disappeared_this_run":len(e["confirmed_disappeared"]),"zone_changes_this_run":len(e["zone_changes"]),"oil_tanker_zone_changes_this_run":len(e["oil_tanker_zone_changes"]),"large_tanker_zone_changes_this_run":len(e["large_tanker_zone_changes"]),"vlcc_ulcc_zone_changes_this_run":len(e["vlcc_ulcc_zone_changes"]),"micro_zone_changes_this_run":len(e["micro_zone_changes"]),"oil_tanker_micro_zone_changes_this_run":len(e["oil_tanker_micro_zone_changes"]),"large_tanker_micro_zone_changes_this_run":len(e["large_tanker_micro_zone_changes"]),"vlcc_ulcc_micro_zone_changes_this_run":len(e["vlcc_ulcc_micro_zone_changes"]),"confirmed_transits_this_run":len(e["confirmed_hormuz_transits"]),"confirmed_micro_transits_this_run":len(e["confirmed_micro_hormuz_transits"]),"possible_direct_transits_this_run":len(e["possible_direct_transits"]),"possible_micro_skipped_core_transits_this_run":len(e["possible_micro_skipped_core_transits"]),"pruned_state_records":len(prune)}
    return {"summary":s,"events":e},state
def count(rows,key):return dict(sorted(Counter(v["position"].get(key) for v in rows if v["position"].get(key)).items()))
def analyze(vs):
    oil=[v for v in vs if v["oil_related"]];cr=[v for v in vs if v["crude_related"]];pr=[v for v in vs if v["product_related"]];large=[v for v in oil if (v["dwt"] or 0)>=100000];vl=[v for v in oil if v["tanker_size"] in("VLCC","ULCC")];known=[v for v in oil if v["dwt"] is not None]
    return {"total_vessels":len(vs),"identified_vessels":len(vs),"oil_related_vessels":len(oil),"crude_related_vessels":len(cr),"product_related_vessels":len(pr),"large_oil_tankers":len(large),"vlcc_ulcc_count":len(vl),"known_oil_tanker_dwt_count":len(known),"total_observed_oil_tanker_dwt":round(sum(v["dwt"] for v in known),2),"all_vessels_by_zone":count(vs,"region"),"oil_tankers_by_zone":count(oil,"region"),"crude_tankers_by_zone":count(cr,"region"),"vlcc_ulcc_by_zone":count(vl,"region"),"all_vessels_by_micro_zone":count(vs,"micro_zone"),"oil_tankers_by_micro_zone":count(oil,"micro_zone"),"crude_tankers_by_micro_zone":count(cr,"micro_zone"),"vlcc_ulcc_by_micro_zone":count(vl,"micro_zone"),"tanker_size_counts":dict(sorted(Counter(v["tanker_size"] for v in oil).items())),"ship_category_counts":dict(sorted(Counter(v["ship_category"] for v in vs).items()))}
def main():
    t=now();p=load_previous();vs=[normalize(r) for r in fetch()];a=analyze(vs);tr,state=track(vs,p,t);s=tr["summary"];hist=p.get("snapshot_history") if isinstance(p.get("snapshot_history"),list) else []
    hist.append({"generated_at":t,"total_vessels":a["total_vessels"],"oil_related_vessels":a["oil_related_vessels"],"crude_related_vessels":a["crude_related_vessels"],"large_oil_tankers":a["large_oil_tankers"],"vlcc_ulcc_count":a["vlcc_ulcc_count"],"oil_tankers_by_zone":a["oil_tankers_by_zone"],"vlcc_ulcc_by_zone":a["vlcc_ulcc_by_zone"],"oil_tankers_by_micro_zone":a["oil_tankers_by_micro_zone"],"vlcc_ulcc_by_micro_zone":a["vlcc_ulcc_by_micro_zone"],"oil_tanker_zone_changes_this_run":s["oil_tanker_zone_changes_this_run"],"oil_tanker_micro_zone_changes_this_run":s["oil_tanker_micro_zone_changes_this_run"],"confirmed_transits_this_run":s["confirmed_transits_this_run"],"confirmed_micro_transits_this_run":s["confirmed_micro_transits_this_run"]})
    churn=round(100*(s["raw_appeared_this_run"]+s["raw_missing_this_run"])/max(s["current_observed_vessels"]+s["raw_missing_this_run"],1),2);issues=[];score=100
    if churn>=25:score-=10;issues.append("High snapshot churn detected; persistent confirmation logic is required.")
    if churn>=60:score-=10;issues.append("Very high snapshot churn; movement interpretation requires extra caution.")
    data={"meta":{"collector":"Hormuz Vessel Monitor","version":VERSION,"generated_at":t,"repository":"energy-data","mode":"MICRO_GEOFENCE_PERSISTENT_VESSEL_TRACKING"},"methodology":{"final_flow_risk_calculated":False,"physical_flow_calculated":False,"new_confirmation_runs":NEW_RUNS,"missing_confirmation_runs":MISSING_RUNS,"max_missing_runs_to_keep":MAX_MISSING,"confirmed_transit_patterns":BROAD_PATTERNS,"confirmed_micro_transit_patterns":MICRO_PATTERNS,"micro_geofence":{"lat_min":LAT0,"lat_max":LAT1,"lon_min":LON0,"lon_max":LON1,"west_max_lon":WEST,"core_max_lon":CORE,"zones":["hormuz_west","hormuz_core","hormuz_east"],"role":"Analytical AIS corridor segmentation; not a legal or navigational boundary."},"principles":["Broad API zones are preserved for backward compatibility.","Micro-zones are calculated independently from coordinates.","NEW requires repeated observation.","DISAPPEARED means repeated AIS absence, not proven physical departure.","Confirmed micro transit requires WEST -> CORE -> EAST or reverse.","AIS absence is not evidence of physical vessel absence.","DWT and vessel counts are logistics indicators, not direct physical oil throughput."]},"source":{"name":"Hormuz API","base_url":BASE,"endpoint":ENDPOINT,"measurement_role":"Persistent vessel-level AIS observation","physical_flow_source":False},"analysis":a,"persistent_tracking":tr,"persistent_vessel_state":state,"snapshot_history":hist[-MAX_HIST:],"data_quality":{"score":score,"status":"HIGH" if score>=85 else "MODERATE","issues":issues,"snapshot_churn_pct":churn,"interpretation":"AIS tracking usability, not physical oil-flow accuracy."},"vessels":vs,"integration":{"integrated_into_hormuz_risk":False,"integrated_into_ompi":False,"ready_for_logistics_model":False,"next_step":"Validate tanker WEST/CORE/EAST transitions before Logistics Stress integration."}}
    OUT.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");print("Wrote",OUT)
if __name__=="__main__":raise SystemExit(main())

