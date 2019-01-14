import { Component, OnInit } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';

// Google Analytics
declare let gtag: Function;

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit {

  constructor(
    public router: Router
  ) {
    this.router.events.subscribe(event => {
      if (event instanceof NavigationEnd) {
        // gtag('set', {'page_path': event.urlAfterRedirects});
      }
    });
  }

  ngOnInit(){
  }

}
