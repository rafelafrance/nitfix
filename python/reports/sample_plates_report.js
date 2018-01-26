// Toggle a group of rows open/closed. That is we will show/hide all records
// for a group. Closing them leaves only the first record (the reconciled one)
// visible.

const tbodyFamilies = document.querySelector('.families tbody');

function toggleFamilyClosed(event) {
  if (! event.target.matches('button')) { return; }
  const family = event.target.dataset.family;
  console.log(family)
  const selector = '.families tr[data-family="' + family + '"]';
  const trs = document.querySelectorAll(selector);
  trs.forEach(function(r) { r.classList.toggle('closed'); });
  // const isClosed = event.target.parentElement.parentElement.classList.contains('closed');
  // groupState[groupBy] = isClosed ? 'closed' : '';
}

tbodyFamilies.addEventListener('click', toggleFamilyClosed);
