document.querySelector('tbody')
  .addEventListener('click', function(event) {
    if (! event.target.matches('button')) { return; }
    event.target.classList.toggle('closed');
    const genus = event.target.dataset.genus;
    const selector = 'tr[data-genus="' + genus + '"]';
    const trs = document.querySelectorAll(selector);
    trs.forEach(function(r) { r.classList.toggle('closed'); });
});
