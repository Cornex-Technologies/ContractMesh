import { useEffect, useRef } from "react";
import { cn } from "../../lib/utils";

export function Button({ variant = "primary", size = "md", className, children, ...props }) {
  return <button className={cn("cc-button", `cc-button-${variant}`, `cc-button-${size}`, className)} {...props}>{children}</button>;
}

export function Badge({ tone = "slate", className, children }) {
  return <span className={cn("cc-badge", `cc-badge-${tone}`, className)}>{children}</span>;
}

export function Card({ className, children, ...props }) {
  return <section className={cn("cc-card", className)} {...props}>{children}</section>;
}

export function CardHeader({ className, children }) {
  return <div className={cn("cc-card-header", className)}>{children}</div>;
}

export function CardTitle({ className, children }) {
  return <h2 className={cn("cc-card-title", className)}>{children}</h2>;
}

export function CardDescription({ className, children }) {
  return <p className={cn("cc-card-description", className)}>{children}</p>;
}

export function CardContent({ className, children }) {
  return <div className={cn("cc-card-content", className)}>{children}</div>;
}

export function Separator({ className }) {
  return <div role="separator" className={cn("cc-separator", className)} />;
}

export function ScrollArea({ className, children }) {
  return <div className={cn("cc-scroll-area", className)}>{children}</div>;
}

export function Dialog({ open, onOpenChange, title, description, children }) {
  const dialogRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event) => {
      if (event.key === "Escape") onOpenChange(false);
    };
    const previous = document.activeElement;
    document.addEventListener("keydown", onKeyDown);
    dialogRef.current?.focus();
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      previous?.focus?.();
    };
  }, [open, onOpenChange]);

  if (!open) return null;
  return (
    <div className="cc-dialog-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onOpenChange(false)}>
      <div className="cc-dialog" role="dialog" aria-modal="true" aria-labelledby="dialog-title" tabIndex={-1} ref={dialogRef}>
        <div className="cc-dialog-header">
          <div>
            <h2 id="dialog-title">{title}</h2>
            {description ? <p>{description}</p> : null}
          </div>
          <Button variant="ghost" size="icon" aria-label="Close dialog" onClick={() => onOpenChange(false)}>×</Button>
        </div>
        {children}
      </div>
    </div>
  );
}

export function Skeleton({ className }) {
  return <div className={cn("cc-skeleton", className)} aria-hidden="true" />;
}

export function EmptyState({ icon: Icon, text }) {
  return <div className="react-empty-state">{Icon ? <Icon size={17} /> : null}<span>{text}</span></div>;
}
