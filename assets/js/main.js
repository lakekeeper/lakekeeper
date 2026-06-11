(function ($) {
  'use strict';

 

  /*--------------------------------------------------------------
    Scripts initialization
  --------------------------------------------------------------*/
  $.exists = function (selector) {
    return $(selector).length > 0;
  };

  $(window).on('load', function () {
    preloader();
  });

  $(function () {
    
    
    
    counterInit();
    
    
    if ($.exists('.wow')) {
      new WOW().init();
    }
  });

  $(window).on('scroll', function () {
    stickyHeader();
  });

  /*--------------------------------------------------------------
    1. Preloader
  --------------------------------------------------------------*/
  function preloader() {
    $('.cs_preloader').fadeOut();
    $('cs_preloader_in').delay(150).fadeOut('slow');
  }



  /*--------------------------------------------------------------
    3. Sticky Header
  --------------------------------------------------------------*/
  function stickyHeader() {
    var scroll = $(window).scrollTop();
    if (scroll >= 10) {
      $('.cs_site_header').addClass('cs_sticky_active');
    } else {
      $('.cs_site_header').removeClass('cs_sticky_active');
    }
  }

 
 
  /*--------------------------------------------------------------
    6. Counter Animation
  --------------------------------------------------------------*/
  function counterInit() {
    if ($.exists('.odometer')) {
      $(window).on('scroll', function () {
        function winScrollPosition() {
          var scrollPos = $(window).scrollTop(),
            winHeight = $(window).height();
          var scrollPosition = Math.round(scrollPos + winHeight / 1.2);
          return scrollPosition;
        }

        $('.odometer').each(function () {
          var elemOffset = $(this).offset().top;
          if (elemOffset < winScrollPosition()) {
            $(this).html($(this).data('count-to'));
          }
        });
      });
    }
  }

  

  
})(jQuery); // End of use strict
