angular.module('MyApp')
    .factory('Show', function ($resource) {
        return $resource('/api/shows/:_id');
    })
    .factory('Show_names', function ($resource) {
        return $resource('/api/shows_names/:_id');
    });