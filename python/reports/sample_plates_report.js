// Toggle a group of rows open/closed. That is we will show/hide all genus records
// for a family. Closing them leaves only the first record (the family one) visible.
document.querySelector('.families tbody')
  .addEventListener('click', function(event) {
    if (! event.target.matches('button')) { return; }
    const family = event.target.dataset.family;
    const selector = '.families tr[data-family="' + family + '"]';
    const trs = document.querySelectorAll(selector);
    trs.forEach(function(r) { r.classList.toggle('closed'); });
});


// Toggle the report section closed
document.querySelectorAll('h2 > button').forEach(function(header) {
  header.addEventListener('click', function(event) {
    event.target.classList.toggle('closed');
    const selector = '.' + event.target.dataset.selector;
    document.querySelectorAll(selector).forEach(function(t) {
      t.classList.toggle('closed');
    });
  });
});
