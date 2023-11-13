% Based on rating.py
clc
clear all
close all

%# -*- coding: utf-8 -*-
%""" This module defines stream rating functions.

%"""

%__version__ = '0.0.1'
%__author__  = 'Tim Hodson'

%from math import *
%import numpy as np

%num = xlsread('TileDischarge_PRC_S148_10132015_12032017');
num = xlsread('TileDischarge_PRC_S148_10132015_03282018');
% Get the date - 1 and adjust year
date = num(5:end,1)-1+(1900*(365)+461);
% datestr(date)
depth_up = num(5:end,4);   % [mm]
depth_down = num(5:end,7); % [mm]
PPT = num(5:end,13); % [mm]

g = 9.81; % Acceleration of gravity

 
% def rating_148(depth_up, depth_down):
%     """Calculate discharge at station 148.
% 
%     Function that calculates discharge from station 148 based on water depth
%     measurements taken upstream and downstream of the weir. The typically weir
%     equation has been modified to account for the fact that flow occurs through
%     a triangular orifice. The functions uses a different equation for each of
%     the three flow states. If water level is below the weir, there is no flow.
%     If water level is within the weir notch, discharge is calculated from a
%     standard v-notch weir equation. Otherwise, if water level rises above the
%     weir, an orifice flow equation is used.
% 
%     Args:
%         depth_up (array): Water depth upstream of the weir.
%         depth_down (array): Water depth upstream of the weir.
% 
%     Returns:
%         Q (array): Calculated discharge.
%     """
%     # The height of the datum - the bottom of the weir notch -
%     # above the upstream transducer. 
     bhead = 220.6/1000; %#m
% 
%     # remove offset between upstream and downstream pressure transducers
     depth_down = depth_down + 24; %#mm
% 
%     # Weir top and bottom in m relative to the datum
     weir_top = (392.8/1000) - bhead; %# m
     weir_bottom = 0;
% 
%     # discharge coefficient for a 45-degree v-notch
     C_d = 0.58;
%     # head correction for a 45-degree v-notch
     k_h = 15/1000; % #m
%     # height from notch to top of weir
     H_w = 158.5/1000; %#m
%     # notch angle in degrees
     theta = 45;
% 
%     # constant portion of the v-notch weir equation
     %K_weir = C_d * (8/15)*sqrt(2*g) * tan(radians(theta/2));
     K_weir = C_d * (8/15)*sqrt(2*g) * tan(degtorad(theta/2));
     
%     # convert water depth in mm to heigh above datum in m
     h_up =  (depth_up/1000) - bhead;
     h_down = (depth_down/1000) - bhead;
% 
%     # Calculate discharge assuming v-notch weir
     %Q = K_weir * (h_up + k_h)**(5/2)
     Q = K_weir * (h_up + k_h).^(5/2);
% 
%     # if the water rises above the weir top, switch to orifice flow
     %Q[h_up > weir_top] = (C_d * H_w ** 2 * tan(radians(theta/2))
     %                      * np.sqrt(2 * g * (h_up - h_down)[h_up > weir_top]))
     hup_down = h_up - h_down;
     Q(h_up > weir_top) = (C_d * H_w .^ 2 * tan(degtorad(theta/2)) ...
                           * sqrt(2 * g * hup_down(h_up > weir_top)));
% 
%     # else if water is below the weir notch, there is no flow
     %Q[h_up < weir_bottom] = 0
     Q(h_up < weir_bottom) = 0;
% 
%     return Q
% 
% 
% def get_rating(station):
%     """Returns the rating curve of a given station.
% 
%     Args:
%         station (str): Three digit ISWS station number.
% 
%     Returns:
%         rating (function): Rating function for the given station.
%     """
% 
%     rating = {
%     '148': rating_148
%     }[station]
% 
%     return rating
