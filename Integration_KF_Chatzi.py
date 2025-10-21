# Original MATLAB code
"""
function [d, v, P] = Integration_KF_Chatzi(a,Ts, Q, R, d0, v0, P0)
    N = size(a,1);
    
    A = [1 Ts; 0 1];
    %B = [0 1/2*Ts^2; 0 Ts];
    % use "simplified" way: only the second column of the [B]
    B = [1/2*Ts^2; Ts]; 
    C = [1 0];
    
    x_hat = NaN(2,N);
    if nargin==4
        % Since I don't know v_0 and y_0    
        x_hat(:,1) = [0; 0];
        P = eye(2);
    else
        x_hat(:,1) = [d0; v0];
        P = P0;
    end
    Y = 0;
    I2 = eye(2);
    for k=2:N
        % Prediction step
        x_minus_hat = A*x_hat(:,k-1) + B*a(k-1);
        %P_minus = A*P*A' + Q; % B*Q*B'; -- the difference is perhaps due to different formulation of the process noise in the Eleni's and Sharkh's papers
        P_minus = A*P*A' + B*Q*B'; 
    
        % Correction step
        K = (P_minus*C')/(C*P_minus*C'+R);
        x_hat(:,k) = x_minus_hat + K*(Y-C*x_minus_hat);
        P = (I2-K*C)*P_minus;
    end
    d = x_hat(1,:)';
    v = x_hat(2,:)';
end
"""

import numpy as np


def Integration_KF_Chatzi(a, Ts, Q, R, d0=0, v0=0, P0=np.eye(2)):
    """
    This function implements the Kalman Filter for integration.

    Parameters:
    a: The acceleration input
    Ts: The sampling time
    Q: The process noise covariance
    R: The measurement noise covariance
    d0: The initial displacement (default is 0)
    v0: The initial velocity (default is 0)
    P0: The initial error covariance matrix (default is identity matrix)

    Returns:
    d: The displacement estimates
    v: The velocity estimates
    P: The final error covariance matrix
    """
    N = len(a)

    A = np.array([[1, Ts], [0, 1]])
    B = np.array([0.5 * Ts**2, Ts])
    C = np.array([1, 0])

    x_hat = np.zeros((2, N))
    x_hat[:, 0] = [d0, v0]
    P = P0
    Y = 0
    I2 = np.eye(2)

    for k in range(1, N):  # MATLAB: for k=2:N
        # Prediction step
        x_minus_hat = A @ x_hat[:, k-1] + B * a[k-1]  # MATLAB: x_minus_hat = A*x_hat(:,k-1) + B*a(k-1);
        # Note the difference with my MATLAB code, this is okay!
        P_minus = A @ P @ A.T + np.outer(B, B) * Q  # MATLAB: P_minus = A*P*A' + B*Q*B';
        # P_minus = A @ P @ A.T + Q  # % -- the difference is perhaps due to different formulation of the process noise in the Eleni's and Sharkh's papers
        # Correction step
        K = (P_minus @ C) / (C @ P_minus @ C + R)
        x_hat[:, k] = x_minus_hat + K * (Y - C @ x_minus_hat)  # MATLAB: x_hat(:,k) = x_minus_hat + K*(Y-C*x_minus_hat);
        P = (I2 - np.outer(K, C)) @ P_minus

    d = x_hat[0, :]
    v = x_hat[1, :]

    return d, v, P

"""```
This Python function does the same thing as your MATLAB function. 
It implements a Kalman Filter for integration, which is used to estimate the displacement and velocity from acceleration data. 
The Kalman Filter consists of two steps: the prediction step and the correction step. 
In the prediction step, the next state is predicted using the current state and the control input. 
In the correction step, the predicted state is corrected using the measurement. 
The error covariance matrix `P` is updated in each step. 
The function returns the displacement estimates `d`, the velocity estimates `v`, and the final error covariance matrix `P`. 
The initial displacement `d0`, initial velocity `v0`, and initial error covariance matrix `P0` can be specified as optional parameters. 
If they are not specified, the initial displacement and velocity are assumed to be 0, and the initial error covariance matrix is assumed to be the identity matrix. 
The function uses the NumPy library for matrix operations. 
The `@` operator is used for matrix multiplication, and the `np.outer` function is used to compute the outer product of two vectors. 
The `np.eye` function is used to create an identity matrix. 
The `np.zeros` function is used to create a matrix of zeros. 
The `np.array` function is used to create a matrix from a list of lists. 
The `len` function is used to get the number of elements in the acceleration input `a`.
"""