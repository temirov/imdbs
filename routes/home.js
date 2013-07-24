
/*
 * GET home page.
 */

module.exports = function initHome (app) {
  app.get('/home', function(req, res) {
    res.redirect('/');
  });
  app.get('/', function(req, res) {
    res.render('index', { title: 'imdbS' });
  });
};