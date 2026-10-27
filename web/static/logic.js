// Pure display rules; no statistics or model probabilities are recomputed here.
export function rankRows(rows, metric, countKey, keys) {
  return [...rows].sort((a,b) => (b[metric] ?? -Infinity)-(a[metric] ?? -Infinity) ||
    b[countKey]-a[countKey] || keys.reduce((order,key) => order || a[key]-b[key],0));
}
export function eligibleRows(rows, includeInsufficient) {
  return includeInsufficient ? rows : rows.filter(row => row.sample_status === 'eligible');
}
export function pageRows(rows, requestedPage, size=20) {
  const pages = Math.max(1,Math.ceil(rows.length/size));
  const page = Math.max(1,Math.min(requestedPage,pages));
  return {rows: rows.slice((page-1)*size,page*size),page,pages,total:rows.length};
}
export function validDraft(radiant,dire) {
  const ids=[...radiant,...dire];
  return radiant.length===5 && dire.length===5 && ids.every(id=>Number.isInteger(id)&&id>0) && new Set(ids).size===10;
}
