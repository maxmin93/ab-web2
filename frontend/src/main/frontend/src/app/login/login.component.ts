import { Component, OnInit, AfterViewInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { HttpErrorResponse } from '@angular/common/http';

import { MatSnackBar } from '@angular/material';

import { Observable } from 'rxjs';
import * as _ from 'lodash';

import { AgensDataService } from '../services/agens-data.service';
import { IResponseDto } from '../models/agens-response-types';

import * as CONFIG from '../app.config';

// Google Analytics
declare let gtag: Function;

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit, AfterViewInit {

  private waitTime: number = 2000;
  private returnUrl: string;

  constructor(
    private _route: ActivatedRoute,
    private _router: Router,
    private _api: AgensDataService,
    public _snackBar: MatSnackBar
  ) { 
  }

  ngOnInit(){
    // initialize ssid
    localStorage.removeItem(CONFIG.USER_KEY);
    // get return url from route parameters or default to '/'
    this.returnUrl = this._route.snapshot.queryParams['returnUrl'] || '/';

    this.login();
  }

  ngAfterViewInit(){
    gtag('set', {'page_title':'login', 'page_path':'/login', 'screen_name':'login'});
    gtag('event', 'screen_view');
  }

  login() {
    let connect$:Observable<boolean> = this._api.auth_connect();

    connect$.subscribe(
      x => { 
        console.log('auth.connect: return =', x);
        if( x ){
          // after a few minitues, start navigation
          setTimeout(() => {
            console.log(`wait ${this.waitTime/1000} seconds..`);            
            // returnUrl 로 이동
            this._router.navigate([this.returnUrl]);
          }, this.waitTime);

          gtag('event', 'loginOk', {'event_category':'login', 'event_label':'login'});
        }
        else{
          setTimeout(() => {
            console.log(`retry login after ${this.waitTime/1000} seconds..`);
            // login 페이지로 이동
            this._router.navigate(['/login']);
          }, this.waitTime);

          gtag('event', 'loginRetry', {'event_category':'login', 'event_label':'login'});
        } 
      },
      err => {
        console.log( 'auth.login: ERROR=', err instanceof HttpErrorResponse, err.error );
        this._api.setResponses(<IResponseDto>{
          group: 'auth.connect',
          state: err.statusText,
          message: (err instanceof HttpErrorResponse) ? err.error.message : err.message
        });

        setTimeout(() => {
          console.log(`retry login after ${this.waitTime/1000} seconds..`);
          // login 페이지로 이동
          this._router.navigate(['/login']);
        }, this.waitTime);

        gtag('event', 'loginFail', {'event_category':'login', 'event_label':'login'});
    });
  }


}
