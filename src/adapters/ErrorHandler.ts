import { ValidationError } from "class-validator";
import pino from "pino";

export class AppError extends Error {
  public readonly statusCode: number;
  public readonly errors?: ValidationError[];

  constructor(message: string, statusCode = 500, errors?: ValidationError[]) {
    super(message);
    this.statusCode = statusCode;
    this.errors = errors;
  }
}

const extractValidationErrors = (validationErrors: ValidationError[]): string[] => {
  const errors: string[] = [];

  const processError = (error: ValidationError, path: string = "") => {
    const currentPath = path ? `${path}.${error.property}` : error.property;

    if (error.constraints) {
      Object.entries(error.constraints).forEach(([_, message]) => {
        errors.push((message as string).replace(error.property, currentPath));
      });
    }
    if (error.children && error.children.length > 0) {
      error.children.forEach((childError: ValidationError) => {
        processError(childError, currentPath);
      });
    }
  };

  validationErrors.forEach((error) => processError(error));
  return errors;
};

export type HandledError = {
  message: string;
  errors: string[];
  statusCode: number;
  stack?: string;
  isOperational: boolean;
};

export class ErrorHandler {
  constructor(private readonly logger: pino.Logger<never, boolean>) {}

  handleError(error: Error, context: any = {}): HandledError {
    const errorMessage = error.message
      .substring(0, 240)
      .replace(/(\r\n|\n|\r)/gm, " ");

    const isOperational = error instanceof AppError;

    const logData: any = {
      err: { message: errorMessage, stack: error.stack },
      context,
    };

    if (error instanceof AppError && error.errors) {
      logData.validationErrors = error.errors.map((validationError) => ({
        property: validationError.property,
        constraints: validationError.constraints,
        value: validationError.value,
      }));
    }

    if (isOperational) {
      this.logger.warn(
        logData,
        `Message processing rejected on queue "${context?.queue ?? context?.exchange ?? "unknown"}": ${errorMessage}`
      );
    } else {
      this.logger.error(
        logData,
        `Unexpected error while processing message on queue "${context?.queue ?? context?.exchange ?? "unknown"}": ${errorMessage}`
      );
    }

    if (error instanceof AppError) {
      const errors: string[] = error.errors
        ? extractValidationErrors(error.errors)
        : [error.message];

      return {
        message: errorMessage,
        errors,
        statusCode: error.statusCode,
        stack: error.stack,
        isOperational: true,
      };
    }

    return {
      message: "Internal server error",
      errors: ["Internal server error"],
      statusCode: 500,
      stack: error.stack,
      isOperational: false,
    };
  }
}
